"""
Prepare eo3 metadata for a Copernicus Global Land Service -
Lake Water Quality dataset.
"""

import warnings
from datetime import datetime

import rioxarray
from eodatasets3.images import ValidDataMethod
from eodatasets3.model import DatasetDoc
from odc.apps.dc_tools._docs import odc_uuid

from deafrica.data.cgls_lwq.geotiff import parse_dataset_tile_id
from deafrica.data.easi_assemble import EasiPrepare


def get_common_attrs(geotiff_url: str) -> dict:
    common_attrs = rioxarray.open_rasterio(geotiff_url).attrs
    return common_attrs


def prepare_dataset(
    tile_id: str,
    dataset_path: str,
    product_yaml: str,
    output_path: str,
) -> DatasetDoc:
    """Prepare an eo3 metadata file for a data product.

    Parameters
    ----------
    tile_id : str
        Unique tile ID for a single dataset to prepare.
    dataset_path : str
        Directory of the datasets
    product_yaml : str
        Path to the product definition yaml file.
    output_path : str
        Path to write the output eo3 metadata file.

    Returns
    -------
    DatasetDoc
        eo3 metadata document
    """
    ## Initialise and validate inputs
    # Creates variables (see EasiPrepare for others):
    # - p.dataset_path
    # - p.product_name
    p = EasiPrepare(dataset_path, product_yaml, output_path)

    ## File format of data
    # e.g. cloud-optimised GeoTiff (= GeoTiff)
    file_format = "GeoTIFF"
    extension = "tif"

    # Find measurement paths
    tile_id_regex = rf"{tile_id}_(.*?)\.{extension}$"
    measurement_map = p.map_measurements_to_paths(tile_id_regex)

    # Get attrs from one of the measurement files
    common_attrs = get_common_attrs(list(measurement_map.values())[0])

    ## IDs and Labels
    # The version of the source dataset
    p.dataset_version = f"v{common_attrs['product_version']}"
    p.dataset_id = odc_uuid(p.product_name, p.dataset_version, [tile_id])
    # product_name is added by EasiPrepare().init()
    p.product_uri = f"https://explorer.digitalearth.africa/product/{p.product_name}"

    ## Satellite, Instrument and Processing level
    # High-level name for the source data (satellite platform or project name).
    # Comma-separated for multiple platforms.
    p.platform = common_attrs["platform"]
    #  Instrument name, optional
    p.instrument = common_attrs["sensor"]
    # Organisation that produces the data.
    # URI domain format containing a '.'
    # Plymouth Marine Laboratory and Brockmann Consult
    p.producer = "https://pml.ac.uk/, https://www.brockmann-consult.de/"
    # ODC/EASI identifier for this "family" of products, optional
    p.product_family = "cgls_water_quality"

    ## Scene capture and Processing
    # Searchable datetime of the dataset, datetime object
    p.datetime = datetime.strptime(
        common_attrs["time_coverage_start"], "%d-%b-%Y %H:%M:%S"
    )
    # Searchable start and end datetimes of the dataset, datetime objects
    p.datetime_range = (
        datetime.strptime(common_attrs["time_coverage_start"], "%d-%b-%Y %H:%M:%S"),
        datetime.strptime(common_attrs["time_coverage_end"], "%d-%b-%Y %H:%M:%S"),
    )
    # When the source dataset was created by the producer, datetime object
    p.processed = datetime.fromisoformat(common_attrs["processing_time"])

    ## Geometry
    # Geometry adds a "valid data" polygon for the scene, which helps bounding box searching in ODC
    # Either provide a "valid data" polygon or calculate it from all bands in the dataset
    # ValidDataMethod.thorough = Vectorize the full valid pixel mask as-is
    # ValidDataMethod.filled = Fill holes in the valid pixel mask before vectorizing
    # ValidDataMethod.convex_hull = Take convex-hull of valid pixel mask before vectorizing
    # ValidDataMethod.bounds = Use the image file bounds, ignoring actual pixel values
    # p.geometry = Provide a "valid data" polygon rather than read from the file, shapely.geometry.base.BaseGeometry()
    # p.crs = Provide a CRS string if measurements GridSpec.crs is None, "epsg:*" or WKT
    p.valid_data_method = ValidDataMethod.bounds

    # Helpful but not critical
    p.properties["odc:file_format"] = file_format
    p.properties["odc:product"] = p.product_name

    ## Scene metrics, as available

    # The "region" of acquisition, if applicable
    _, _, _, _, _, _, tile_index_str = parse_dataset_tile_id(tile_id)
    p.region_code = tile_index_str

    ## Ignore warnings, OPTIONAL
    # Ignore unknown property warnings (generated in eodatasets3.properties.Eo3Dict().normalise_and_set())
    # Eodatasets3 validates properties against a hardcoded list, which includes DEA stuff so no harm if we add our own
    custom_prefix = "cgls_lwq"  # usually related to the product name or type
    warnings.filterwarnings(
        "ignore", message=f".*Unknown stac property.+{custom_prefix}:.+"
    )
    ## Product-specific properties, OPTIONAL
    # For examples see eodatasets3.properties.Eo3Dict().KNOWN_PROPERTIES
    p.properties[f"{custom_prefix}:processing_centre"] = common_attrs[
        "processing_centre"
    ]
    p.properties[f"{custom_prefix}:processing_level"] = common_attrs["processing_level"]
    p.properties[f"{custom_prefix}:processor"] = common_attrs["processor"]

    product_type = common_attrs.get("product_type")
    if product_type:
        p.properties[f"{custom_prefix}:product_type"] = common_attrs["product_type"]

    p.properties[f"{custom_prefix}:title"] = common_attrs["title"]
    p.properties[f"{custom_prefix}:trackingid"] = common_attrs["trackingID"]

    # Add measurement paths
    for measurement_name, file_location in measurement_map.items():
        p.note_measurement(
            measurement_name=measurement_name,
            file_path=file_location,
            expand_valid_data=True,
            relative_to_metadata=False,
        )
    return p.to_dataset_doc(validate_correctness=True, sort_measurements=True)
