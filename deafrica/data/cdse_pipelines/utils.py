import json
import time
from importlib.resources import files

from deafrica.logs import setup_logging

SH_BATCH_URL = "https://sh.dataspace.copernicus.eu/api/v2/batch/process"

log = setup_logging()


def load_evalscript(path):
    return files("deafrica.data.cdse_pipelines").joinpath(path).read_text()


def poll_status(
    session, job_id: str, terminal: set, interval: int = 30, timeout: int = 3600
) -> dict:
    "Poll the batch job until it reaches one of the given terminal states."
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = session.get(f"{SH_BATCH_URL}/{job_id}", timeout=60)
        resp.raise_for_status()
        info = resp.json()
        status = info.get("status")
        log.info(f"  job {job_id}: {status}")
        if status in terminal:
            return info
        time.sleep(interval)
    raise TimeoutError(f"Job {job_id} did not reach {terminal} within {timeout}s")


def submit_one(
    session,
    sat_config: dict,
    date: str,
    job_timeliness: str,
    delivery_url: str,
    description: str,
    aoi_bbox: list[float] | None,
    dry_run: bool = False,
) -> dict:
    """
    Create, analyse, and start the batch job for one date. Returns
    {"date", "job_id", "timeliness", "status"}; raises on any failure so the
    caller can record it and continue with the remaining dates.

    The caller resolves job_timeliness (product-specific catalog query) and
    builds delivery_url (the full BatchV2 template ending in
    ".../<tileName>/<outputId>.<format>") and description before calling.

    Handles jobs whose analysis lands directly in DONE (all outputs already
    exist at the delivery prefix, costPU=0, nothing to start) as success
    rather than an error.
    """

    from .payloads import build_batch_payload, redact_payload

    payload = build_batch_payload(
        sat_config=sat_config,
        time_from=f"{date}T00:00:00Z",
        time_to=f"{date}T23:59:59Z",
        bbox=aoi_bbox,
    )
    payload["description"] = description

    # Physical delivery to the CF staging bucket (--output-bucket)
    payload["output"]["delivery"]["s3"]["url"] = delivery_url

    if dry_run:
        print(json.dumps(redact_payload(payload), indent=2))
        return {"date": date, "timeliness": job_timeliness, "status": "dry_run"}

    resp = session.post(SH_BATCH_URL, json=payload)
    if not resp.ok:
        # surface the API's validation message - batch 400s are usually specific
        log.error(f"Create failed ({resp.status_code}): {resp.text[:2000]}")
        resp.raise_for_status()
    job_id = resp.json()["id"]
    log.info(f"Created job {job_id}")

    # Analyse before starting - validates the geopackage and job parameters
    analyse = session.post(f"{SH_BATCH_URL}/{job_id}/analyse")
    if not analyse.ok:
        log.error(f"Analyse failed ({analyse.status_code}): {analyse.text[:2000]}")
        analyse.raise_for_status()

    info = poll_status(
        session,
        job_id,
        terminal={"ANALYSIS_DONE", "PROCESSING", "DONE", "FAILED", "CANCELED"},
        timeout=900,
    )
    status = info.get("status")

    if status in ("FAILED", "CANCELED"):
        if info.get("error"):
            log.error(f"Analysis error: {info.get('error')}")
        log.error(f"Analysis did not complete: {json.dumps(info, indent=2)[:2000]}")
        raise RuntimeError(f"Job {job_id} analysis ended in {status}")

    if status == "ANALYSIS_DONE":
        start = session.post(f"{SH_BATCH_URL}/{job_id}/start")
        if not start.ok:
            log.error(f"Start failed ({start.status_code}): {start.text[:2000]}")
            start.raise_for_status()
        log.info(f"Started job {job_id}")
    else:
        # DONE and costPU > 0 - real job already finished
        log.info(
            f"Job {job_id} already {status} after analysis "
            f"(completion={info.get('completionPercentage')}%, "
            f"costPU={info.get('costPU')}); skipping explicit start"
        )
        # DONE and costPU = 0 - outputs already exist
        if status == "DONE" and not info.get("costPU"):
            log.info(
                f"{date}: costPU=0 - all outputs already existed at the "
                f"delivery prefix; nothing was (re)rendered"
            )

    # ".../{YYYY}/{MM}" prefix: strip "/<dataset-folder>/<outputId>.<format>"
    log.info(
        f"Outputs will be delivered under {delivery_url.rsplit('/', 2)[0]}/ "
        f"- same keys as the final archive"
    )

    return {
        "date": date,
        "job_id": job_id,
        "timeliness": job_timeliness,
        "status": "started",
    }
