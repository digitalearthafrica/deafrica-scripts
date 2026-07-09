import copy
import os

from .utils import load_evalscript


def _build_data_block(sat_config: dict, time_from: str, time_to: str) -> dict:
    """
    Build the processRequest block.

    Optional dataFilter keys (acquisitionMode, polarization, resolution) and
    the processing block are only included when set in sat_config.
    """
    data_filter = {"timeRange": {"from": time_from, "to": time_to}}
    for key in ("acquisitionMode", "polarization", "resolution"):
        if sat_config.get(key) is not None:
            data_filter[key] = sat_config[key]

    data_block = {"type": sat_config["type"], "dataFilter": data_filter}
    if sat_config.get("processing") is not None:
        data_block["processing"] = sat_config["processing"]
    return data_block


def _build_input_block(sat_config: dict) -> dict:
    """
    Build the batch-level input block from sat_config["input"].

    Two supported types:
      - tiling-grid: passed through as-is ({type, id, resolution}).
      - geopackage : deep-copied, with S3 credentials injected (defaulting
        to the delivery credentials from the environment) unless the config
        already hardcodes keys for a gpkg living in another bucket.
    """
    cfg = sat_config.get("input")
    if not cfg:
        raise ValueError(
            f"sat_config for {sat_config.get('type')} has no 'input' block; "
            "define a tiling-grid or geopackage input"
        )

    block = copy.deepcopy(cfg)
    if block["type"] == "geopackage":
        s3 = block["features"]["s3"]
        s3.setdefault("accessKey", os.environ["S3_ACCESS_KEY"])
        s3.setdefault("secretAccessKey", os.environ["S3_SECRET_ACCESS_KEY"])
    elif block["type"] != "tiling-grid":
        raise ValueError(f"Unsupported batch input type: {block['type']}")
    return block


def build_batch_payload(sat_config, time_from, time_to, *, bbox=None, geometry=None):
    """
    Build a CDSE Batch API payload (input + processRequest + output).

    The batch-level input (tiling grid or geopackage) comes from
    sat_config["input"].

    AOI (`bbox` or `geometry`, both EPSG:4326, at most one):
      - tiling-grid input: REQUIRED; tiles intersecting the AOI are processed.
      - geopackage input : OPTIONAL; if omitted, ALL features in the gpkg are
        processed. If given, only intersecting features are processed (and
        charged). Partially covered features are processed in full.
    """
    input_block = _build_input_block(sat_config)
    is_gpkg = input_block["type"] == "geopackage"

    if bbox is not None and geometry is not None:
        raise ValueError("Provide at most one of bbox or geometry")
    if not is_gpkg and bbox is None and geometry is None:
        raise ValueError("Tiling-grid input requires a bbox or geometry AOI")

    bounds = None
    if bbox is not None or geometry is not None:
        bounds = {"properties": {"crs": "http://www.opengis.net/def/crs/EPSG/0/4326"}}
        if bbox is not None:
            bounds["bbox"] = bbox
        else:
            bounds["geometry"] = geometry

    # bounds is omitted entirely (not null) when absent - the SH API rejects
    # JSON nulls, same rationale as the optional dataFilter keys above.
    process_input = {"data": [_build_data_block(sat_config, time_from, time_to)]}
    if bounds is not None:
        process_input["bounds"] = bounds

    responses = [
        {"identifier": b, "format": {"type": "image/tiff"}} for b in sat_config["bands"]
    ]
    responses.append({"identifier": "userdata", "format": {"type": "application/json"}})

    product = sat_config.get("odc_product", "batch")

    return {
        "input": input_block,
        "processRequest": {
            "input": process_input,
            "output": {
                "responses": responses,
            },
            "evalscript": load_evalscript(sat_config["evalscript_file"]),
        },
        "output": {
            "type": "raster",
            "delivery": {
                "s3": {
                    "url": f"s3://cdse_batch_test_bucket/{product}/<tileName>/<outputId>.<format>",
                    "accessKey": os.environ["S3_ACCESS_KEY"],
                    "secretAccessKey": os.environ["S3_SECRET_ACCESS_KEY"],
                }
            },
            "cogOutput": True,
            "skipExisting": True,
        },
        "description": f"Batch {sat_config['type']} job ({product})",
    }


def redact_payload(payload: dict) -> dict:
    """
    Deep-copy a batch payload with all S3 credentials masked, for dry-run
    printing / logging. Handles both the delivery block and (if present)
    the geopackage features block.
    """
    redacted = copy.deepcopy(payload)
    for s3 in (
        redacted.get("output", {}).get("delivery", {}).get("s3"),
        redacted.get("input", {}).get("features", {}).get("s3")
        if redacted.get("input", {}).get("type") == "geopackage"
        else None,
    ):
        if s3:
            if "accessKey" in s3:
                s3["accessKey"] = "***"
            if "secretAccessKey" in s3:
                s3["secretAccessKey"] = "***"
    return redacted
