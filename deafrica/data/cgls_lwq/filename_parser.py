"""
Utilities for parsing CGLS Lake Water Quality (LWQ) filenames and
constructing output COG paths based on product naming conventions.
"""

import os
from datetime import datetime
from itertools import chain

from deafrica.data.cgls_lwq.constants import BAND_NAMES, NAMING_PREFIX
from deafrica.data.cgls_lwq.tiles import get_region_code
from deafrica.io import (
    check_directory_exists,
    find_geotiff_files,
    get_basename,
    get_filesystem,
    join_url,
)


def parse_source_cog_url(
    source_cog_url: str,
) -> tuple[str, str, str, str, str, str, str, str]:
    """
    Parse an LWQ COG URL and return its naming components.

    Parameters
    ----------
    source_cog_url : str
        URL (or path) to the CGLS LWQ COG on EODATA.

    Returns
    -------
    tuple of str
        (prefix, acronym, band_name, date_str, area, sensor, version, extension)

        - prefix: Constant naming prefix, e.g. ``c_gls``
        - acronym: Product acronym (e.g., ``LWQ``)
        - band_name: Band short name embedded in the file name
        - date_str: Timestamp string in ``YYYYMMDDHHMMSS``
        - area: Area
        - sensor: Sensor
        - version: Product version
        - extension: File extension without leading dot
    """
    filename = get_basename(source_cog_url)

    # Get the file extension
    _, extension = os.path.splitext(filename)

    # File naming convention in
    # c_gls_<Acronym>-<BAND>_<YYYYMMDDHHmm>_<AREA>_<SENSOR>_<Version>.<EXTENSION>
    parts = list(
        chain.from_iterable(
            [
                i.split("_")
                for i in filename.removeprefix(NAMING_PREFIX)
                .removesuffix(extension)
                .lstrip("_")
                .split("-")
            ]
        )
    )

    acronym, band_name, date_str, area, sensor, version = parts
    extension = extension.removeprefix(".")

    return (
        NAMING_PREFIX,
        acronym,
        band_name,
        date_str,
        area,
        sensor,
        version,
        extension,
    )


def get_output_cog_url_from_cog(
    product_name: str,
    output_dir: str,
    source_cog_url: str,
    tile_index: tuple[int, int],
) -> str:
    """
    Build the output COG path for a cropped CGLS LWQ COG.

    Parameters
    ----------
    product_name : str
        Name of the CGLS LWQ ODC product (used to resolve band-to-measurement mapping).
    output_dir : str
        Root directory where the cropped COG file will be written.
    source_cog_url : str
        URL (or path) of the source CGLS LWQ COG to be cropped.
    tile_index : tuple[int, int]
        (x, y) index of the tile used to crop the COG.

    Returns
    -------
    str
        Fully qualified path/URL for the output COG.
    """
    filename_prefix, acronym, band_name, date_str, area, sensor, version, extension = (
        parse_source_cog_url(source_cog_url)
    )

    date = datetime.strptime(date_str, "%Y%m%d%H%M%S")
    year = str(date.year)
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"

    region_code = get_region_code(tile_index, sep="/").split("/")

    parent_dir = join_url(output_dir, *region_code, year, month, day)

    if not check_directory_exists(parent_dir):
        fs = get_filesystem(parent_dir, anon=False)
        fs.makedirs(parent_dir, exist_ok=True)

    measurement_name = BAND_NAMES[product_name][band_name]
    file_name = f"{filename_prefix}_{acronym}_{date_str}_{area}_{sensor}_{version}_{get_region_code(tile_index, sep="")}_{measurement_name}.tif"

    output_cog_url = join_url(parent_dir, file_name)

    return output_cog_url


def parse_source_netcdf_url(
    source_netcdf_url: str,
) -> tuple[str, str, str, str, str, str, str]:
    """
    Parse an LWQ NetCDF URL and return its naming components.

    Parameters
    ----------
    source_netcdf_url : str
        URL (or path) to the CGLS LWQ NetCDF on EODATA.

    Returns
    -------
    tuple of str
        (prefix, acronym, date_str, area, sensor, version, extension)

        - prefix: Constant naming prefix, e.g. ``c_gls``
        - acronym: Product acronym (e.g., ``LWQ``)
        - date_str: Timestamp string in ``YYYYMMDDHHMMSS``
        - area: Area
        - sensor: Sensor
        - version: Product version
        - extension: File extension without leading dot
    """
    filename = get_basename(source_netcdf_url)

    # Get the file extension
    _, extension = os.path.splitext(filename)

    # File naming convention in
    # c_gls_<Acronym>_<YYYYMMDDHHmm>_<AREA>_<SENSOR>_<Version>.<EXTENSION>
    parts = (
        filename.removeprefix(NAMING_PREFIX)
        .removesuffix(extension)
        .lstrip("_")
        .split("_")
    )

    acronym, date_str, area, sensor, version = parts
    extension = extension.removeprefix(".")

    return (
        NAMING_PREFIX,
        acronym,
        date_str,
        area,
        sensor,
        version,
        extension,
    )


def get_output_cog_url_from_netcdf(
    product_name: str,
    output_dir: str,
    source_netcdf_url: str,
    subdataset_variable: str,
    tile_index: tuple[int, int],
) -> str:
    """
    Build the output COG path for a cropped subdataset from an LWQ NetCDF.

    Parameters
    ----------
    product_name : str
        Name of the CGLS LWQ ODC product (used to resolve variable-to-measurement mapping).
    output_dir : str
        Root directory where the cropped COG file will be written.
    source_netcdf_url : str
        URL (or path) of the source CGLS LWQ NetCDF.
    subdataset_variable : str
        Variable name within the NetCDF used to create the COG.
    tile_index : tuple[int, int]
        (x, y) index of the tile used to crop the subdataset.

    Returns
    -------
    str
        Fully qualified path/URL for the output COG.
    """
    filename_prefix, acronym, date_str, area, sensor, version, extension = (
        parse_source_netcdf_url(source_netcdf_url)
    )

    date = datetime.strptime(date_str, "%Y%m%d%H%M%S")
    year = str(date.year)
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"

    region_code = get_region_code(tile_index, sep="/").split("/")

    parent_dir = join_url(output_dir, *region_code, year, month, day)

    if not check_directory_exists(parent_dir):
        fs = get_filesystem(parent_dir, anon=False)
        fs.makedirs(parent_dir, exist_ok=True)

    # Map the NetCDF subdataset variable to the product measurement name
    file_name = f"{filename_prefix}_{acronym}_{date_str}_{area}_{sensor}_{version}_{get_region_code(tile_index, sep="")}_{subdataset_variable}.tif"

    output_cog_url = join_url(parent_dir, file_name)

    return output_cog_url


def parse_cog_url(measurement_cog_url: str) -> tuple[str]:
    """
    Parse a CGLS LWQ measurement COG URL and return its naming components.

    Parameters
    ----------
    measurement_cog_url : str
        URL or path to a single measurement GeoTIFF from any ``cgls_lwq*`` ODC product.

    Returns
    -------
    tuple[str]
        (prefix, acronym, date_str, area, sensor, version, region_code,
        measurement_name, extension)

        - prefix: Constant naming prefix, e.g. ``c_gls``
        - acronym: Product acronym (e.g., ``LWQ``)
        - date_str: Timestamp string in ``YYYYMMDDHHMMSS``
        - area: Area
        - sensor: Sensor
        - version: Product version
        - region_code: Tile index string (e.g., ``x015y008``)
        - measurement_name: Measurement/variable name; may contain underscores
        - extension: File extension without leading dot
    """
    filename = get_basename(measurement_cog_url)

    # Get the file extension
    extension = os.path.splitext(filename)[-1]

    # File naming convention in
    # c_gls_<Acronym>_<YYYYMMDDHHmm>_<AREA>_<SENSOR>_<Version>_<region_code>_<subdataset_variable>.<EXTENSION>
    parts = filename.removeprefix(NAMING_PREFIX).removesuffix(extension).split("_")
    parts = list(filter(None, parts))
    acronym, date_str, area, sensor, version, region_code, *subdataset_variable = parts
    subdataset_variable = "_".join(subdataset_variable)

    extension = extension.removeprefix(".")
    return (
        NAMING_PREFIX,
        acronym,
        date_str,
        area,
        sensor,
        version,
        region_code,
        subdataset_variable,
        extension,
    )


def get_dataset_id(dataset_path: str) -> str:
    """Get the unique ID for a CGLS Lake Water Quality dataset.

    Parameters
    ----------
    dataset_path : str
        File path to one of the measurements of the CGLS Lake Water Quality dataset.

    Returns
    -------
    str
        Unique ID for a single CGLS Lake Water Quality dataset
    """
    measurements_cog_urls = find_geotiff_files(dataset_path)
    measurement_cog_url = measurements_cog_urls[0]
    (
        filename_prefix,
        acronym,
        date_str,
        area,
        sensor,
        version,
        region_code,
        _,
        _,
    ) = parse_cog_url(measurement_cog_url)

    dataset_tile_id = f"{filename_prefix}_{acronym}_{date_str}_{area}_{sensor}_{version}_{region_code}"

    return dataset_tile_id


def get_stac_url(dataset_path: str) -> str:
    """
    Return the file path for writing the dataset metadata STAC file.
    """
    dataset_id = get_dataset_id(dataset_path)
    output_path = join_url(dataset_path, f"{dataset_id}.stac-item.json")
    return output_path
