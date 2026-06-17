from .utils import load_evalscript
import os


def build_batch_payload(
    sat_config,
    time_from,
    time_to,
    *,
    bbox=None,
    geometry=None,
    tiling_id=3,
    resolution=0.0002,
):
    """
    Build a CDSE Batch API payload (input + processRequest + output).

    Exactly one of `bbox` or `geometry` must be provided:
      - bbox     : [west, south, east, north] in EPSG:4326
      - geometry : GeoJSON geometry dict in EPSG:4326 (e.g. from shapely.geometry.mapping)
    """
    if (bbox is None) == (geometry is None):
        raise ValueError("Provide exactly one of bbox or geometry")

    bounds = {"properties": {"crs": "http://www.opengis.net/def/crs/EPSG/0/4326"}}
    if bbox is not None:
        bounds["bbox"] = bbox
    else:
        bounds["geometry"] = geometry

    responses = [
        {"identifier": b, "format": {"type": "image/tiff"}} for b in sat_config["bands"]
    ]

    return {
        # =========================
        # BATCH INPUT (tiling config)
        # =========================
        "input": {
            "type": "tiling-grid",
            "id": tiling_id,
            "resolution": resolution,
        },
        # =========================
        # PROCESS REQUEST (Sentinel Hub request)
        # =========================
        "processRequest": {
            "input": {
                "bounds": bounds,
                "data": [
                    {
                        "type": sat_config["type"],
                        "dataFilter": {
                            "timeRange": {
                                "from": time_from,
                                "to": time_to,
                            },
                            "acquisitionMode": sat_config.get("acquisitionMode"),
                            "polarization": sat_config.get("polarization"),
                            "resolution": sat_config.get("resolution"),
                        },
                        "processing": sat_config.get("processing"),
                    }
                ],
            },
            "output": {
                "responses": responses,
            },
            "evalscript": load_evalscript(sat_config["evalscript_file"]),
        },
        # =========================
        # OUTPUT (S3 delivery)
        # =========================
        "output": {
            "type": "raster",
            "delivery": {
                "s3": {
                    "url": "s3://cdse_batch_test_bucket/s1_rtc/<tileName>/<outputId>.<format>",
                    "accessKey": os.environ["S3_ACCESS_KEY"],
                    "secretAccessKey": os.environ["S3_SECRET_ACCESS_KEY"],
                }
            },
            "cogOutput": True,
        },
        "description": "Batch Sentinel-1 Job",
    }
