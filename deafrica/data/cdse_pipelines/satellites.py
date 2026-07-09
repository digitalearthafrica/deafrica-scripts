"""
Per-product configuration for CDSE Batch Processing V2 jobs.
"""

SATELLITES = {
    "s1": {
        "type": "sentinel-1-grd",
        "bands": ["VV", "VH", "AREA", "ANGLE", "MASK"],
        "acquisitionMode": "IW",
        "polarization": "DV",
        "resolution": "HIGH",
        "processing": {
            "upsampling": "BILINEAR",
            "orthorectify": True,
            "demInstance": "COPERNICUS_30",
            "backCoeff": "GAMMA0_TERRAIN",
            "radiometricTerrainOversampling": 2,
        },
        # Batch-level input: WGS84 tiling grid at 0.0002 deg (~20 m).
        "input": {"type": "tiling-grid", "id": 3, "resolution": 0.0002},
        "evalscript_file": "evalscripts/s1_rtc.js",
        "odc_product": "s1_rtc_cdse",
    },
    "s3_lfr": {
        "type": "sentinel-3-olci-l2",
        "bands": ["GIFAPAR", "IWV_L", "OTCI", "RC681", "RC865", "LQSF", "dataMask"],
        "input": {
            "type": "geopackage",
            "features": {
                "s3": {
                    "url": "s3://cdse_batch_test_bucket/gpkg/UTM_Tiles_300m.gpkg",
                }
            },
        },
        "evalscript_file": "evalscripts/s3_lfr.js",
        "odc_product": "s3_lfr",
    },
    "s3_wfr": {
        "type": "sentinel-3-olci-l2",
        "bands": [
            "A865",
            "ADG443_NN",
            "B01",
            "B02",
            "B03",
            "B04",
            "B05",
            "B06",
            "B07",
            "B08",
            "B09",
            "B10",
            "B11",
            "B12",
            "B16",
            "B17",
            "B18",
            "B21",
            "CHL_NN",
            "CHL_OC4ME",
            "IWV_W",
            "KD490_M07",
            "PAR",
            "T865",
            "TSM_NN",
            "dataMask",
        ],
        "processing": {
            "upsampling": "NEAREST",
        },
        "input": {
            "type": "geopackage",
            "features": {
                "s3": {
                    "url": "s3://cdse_batch_test_bucket/gpkg/UTM_Tiles_300m.gpkg",
                }
            },
        },
        "evalscript_file": "evalscripts/s3_wfr.js",
        "odc_product": "s3_olci_l2_wfr",
    },
}
