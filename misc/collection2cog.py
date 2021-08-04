

from datetime import datetime
from pathlib import Path

from eodatasets3.assemble import DatasetAssembler
from eodatasets3.prepare.landsat_l1_prepare import get_mtl_content

LANDSAT_OLI_TIRS_BAND_ALIASES = {
    "1": "coastal_aerosol",
    "2": "blue",
    "3": "green",
    "4": "red",
    "5": "nir",
    "6": "swir_1",
    "7": "swir_2",
    "st_b10": "st_b10",
    "thermal_radiance": "thermal_radiance",
    "upwell_radiance": "upwell_radiance",
    "downwell_radiance": "downwell_radiance",
    "atmospheric_transmittance": "atmospheric_transmittance",
    "emissivity": "emissivity",
    "emissivity_stdev": "emissivity_stdev",
    "cloud_distance": "cloud_distance",
    "quality_l2_aerosol": "quality_l2_aerosol",
    "quality_l2_surface_temperature": "quality_l2_surface_temperature",
    "quality_l1_pixel": "quality_l1_pixel",
    "quality_l1_radiometric_saturation": "quality_l1_radiometric_saturation",
    "metadata_odl": "metadata_odl",
    "metadata_xml": "metadata_xml",
}

# Ensure output path exists
output_location = Path("/g/data/u46/users/dsg547/sandpit/test_scripts/test")
output_location.mkdir(parents=True, exist_ok=True)

adir = "/g/data/u46/users/dsg547/test_data/collection2/LC08_L2SP_185052_20180104_20190821_02_T1/"
acquisition_path = Path(adir)

#acquisition_path.exists()

paths = list(acquisition_path.rglob("*_MTL.txt"))

mtl, _  = get_mtl_content(acquisition_path, root_element="landsat_metadata_file")

#print (mtl)


with DatasetAssembler(output_location, naming_conventions="dea") as p:
    p.properties['eo:instrument'] = mtl['image_attributes']['sensor_id']  # 'OLI_TIRS'
    p.properties['eo:platform'] = mtl['image_attributes']['spacecraft_id'].lower()  # 'landsat_8'
    p.properties['odc:dataset_version'] = '0.0.1'

    p.properties['odc:processing_datetime'] = mtl['level2_processing_record']['date_product_generated']

    p.properties['odc:producer'] = "usgs.gov" #mtl['product_contents']['origin']
    p.properties['odc:product_family'] = mtl['product_contents']['processing_level'].lower()  # 'l2sp'
    apath = int(mtl['image_attributes']['wrs_path'])
    arow = int(mtl['image_attributes']['wrs_row'])
    p.properties['odc:region_code'] = f"{apath:03d}{arow:03d}"

    dt_string = mtl['image_attributes']['date_acquired'] + ' ' + mtl['image_attributes']['scene_center_time'][:-2]
    p.datetime = datetime.strptime(dt_string, '%Y-%m-%d %H:%M:%S.%f')
    p.properties['landsat:landsat_scene_id'] = mtl['level1_processing_record']['landsat_scene_id']
    # p.write_measurement('b1', '/g/data/u46/users/dsg547/test_data/collection2/LC08_L2SP_185052_20180104_20190821_02_T1/LC08_L2SP_185052_20180104_20190821_02_T1_SR_B1.TIF')

   LANDSAT_OLI_TIRS_BAND_ALIASES


    dataset_id, metadata_path = p.done()
    print (dataset_id)