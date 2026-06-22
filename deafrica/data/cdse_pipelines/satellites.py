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
        "tiling_grid": {"type": "tiling-grid", "id": 3, "resolution": 0.0002},
        "evalscript_file": "evalscripts/s1_rtc.js",
        "odc_product": "s1_rtc_cdse",
    }
}
