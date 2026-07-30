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
    "s3_lst": {
        "type": "sentinel-3-slstr-l2",
        "bands": ["LST", "LST_uncertainty", "NDVI", "CONFIDENCE", "dataMask"],
        "input": {
            "type": "geopackage",
            "features": {
                "s3": {
                    "url": "s3://cdse_batch_test_bucket/gpkg/UTM_Tiles_1000m.gpkg",
                }
            },
        },
        "evalscript_file": "evalscripts/s3_lst.js",
        "odc_product": "s3_slstr_l2_lst",
    },
    "s3_vg1": {
        "type": "sentinel-3-synergy-l2",
        "bands": [
            "B0_VG1",
            "B2_VG1",
            "B3_VG1",
            "MIR_VG1",
            "NDVI_VG1",
            "TOA_NDVI_VG1",
            "OG_VG1",
            "SAA_VG1",
            "SZA_VG1",
            "VAA_VG1",
            "VZA_VG1",
            "WVG_VG1",
            "AG_VG1",
        ],
        "input": {
            "type": "geopackage",
            "features": {
                "s3": {
                    "url": "s3://cdse_batch_test_bucket/gpkg/UTM_Tiles_1000m.gpkg",
                }
            },
        },
        "evalscript_file": "evalscripts/s3_vg1.js",
        "odc_product": "s3_syn_2_vg1",
    },
    "s5p_aer_ai": {
        "type": "sentinel-5p-l2",
        "bands": ["AER_AI_340_380", "AER_AI_354_388", "dataMask"],
        "input": {
            "type": "geopackage",
            "features": {
                "s3": {
                    "url": "s3://cdse_batch_test_bucket/gpkg/UTM_Tiles_1000m.gpkg",
                }
            },
        },
        "evalscript_file": "evalscripts/s5p_aer_ai.js",
        "odc_product": "s5p_tropomi_l2_aer_ai",
    },
    "s5p_ch4": {
        "type": "sentinel-5p-l2",
        "bands": ["CH4", "dataMask"],
        "input": {
            "type": "geopackage",
            "features": {
                "s3": {
                    "url": "s3://cdse_batch_test_bucket/gpkg/UTM_Tiles_1000m.gpkg",
                }
            },
        },
        "evalscript_file": "evalscripts/s5p_ch4.js",
        "odc_product": "s5p_tropomi_l2_ch4",
    },
    "s5p_cloud": {
        "type": "sentinel-5p-l2",
        "bands": [
            "CLOUD_BASE_PRESSURE",
            "CLOUD_TOP_PRESSURE",
            "CLOUD_BASE_HEIGHT",
            "CLOUD_TOP_HEIGHT",
            "CLOUD_OPTICAL_THICKNESS",
            "CLOUD_FRACTION",
            "dataMask",
        ],
        "input": {
            "type": "geopackage",
            "features": {
                "s3": {
                    "url": "s3://cdse_batch_test_bucket/gpkg/UTM_Tiles_1000m.gpkg",
                }
            },
        },
        "evalscript_file": "evalscripts/s5p_cloud.js",
        "odc_product": "s5p_tropomi_l2_cloud",
    },
    "s5p_co": {
        "type": "sentinel-5p-l2",
        "bands": ["CO", "dataMask"],
        "input": {
            "type": "geopackage",
            "features": {
                "s3": {
                    "url": "s3://cdse_batch_test_bucket/gpkg/UTM_Tiles_1000m.gpkg",
                }
            },
        },
        "evalscript_file": "evalscripts/s5p_co.js",
        "odc_product": "s5p_tropomi_l2_co",
    },
    "s5p_hcho": {
        "type": "sentinel-5p-l2",
        "bands": ["HCHO", "dataMask"],
        "input": {
            "type": "geopackage",
            "features": {
                "s3": {
                    "url": "s3://cdse_batch_test_bucket/gpkg/UTM_Tiles_1000m.gpkg",
                }
            },
        },
        "evalscript_file": "evalscripts/s5p_hcho.js",
        "odc_product": "s5p_tropomi_l2_hcho",
    },
    "s5p_no2": {
        "type": "sentinel-5p-l2",
        "bands": ["NO2", "dataMask"],
        "input": {
            "type": "geopackage",
            "features": {
                "s3": {
                    "url": "s3://cdse_batch_test_bucket/gpkg/UTM_Tiles_1000m.gpkg",
                }
            },
        },
        "evalscript_file": "evalscripts/s5p_no2.js",
        "odc_product": "s5p_tropomi_l2_no2",
    },
    "s5p_o3": {
        "type": "sentinel-5p-l2",
        "bands": ["O3", "dataMask"],
        "input": {
            "type": "geopackage",
            "features": {
                "s3": {
                    "url": "s3://cdse_batch_test_bucket/gpkg/UTM_Tiles_1000m.gpkg",
                }
            },
        },
        "evalscript_file": "evalscripts/s5p_o3.js",
        "odc_product": "s5p_tropomi_l2_o3",
    },
    "s5p_so2": {
        "type": "sentinel-5p-l2",
        "bands": ["SO2", "dataMask"],
        "input": {
            "type": "geopackage",
            "features": {
                "s3": {
                    "url": "s3://cdse_batch_test_bucket/gpkg/UTM_Tiles_1000m.gpkg",
                }
            },
        },
        "evalscript_file": "evalscripts/s5p_so2.js",
        "odc_product": "s5p_tropomi_l2_so2",
    },
}
