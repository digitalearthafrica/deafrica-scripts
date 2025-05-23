"""
Parse CGLS Lake Water Quality dataset geotiff URLs and filenames.
"""

import os
import posixpath
from urllib.parse import urlparse

from deafrica.data.cgls_lwq.constants import NAMING_PREFIX
from deafrica.io import is_local_path


def parse_geotiff_url(geotiff_url: str) -> tuple[str]:
    """
    Get the filename components of a CGLS Lake Water Quality dataset geotiff

    Parameters
    ----------
    geotiff_url : str
        CGLS Lake Water Quality datataset geotiff file path

    Returns
    -------
    tuple[str]
        Filename components of a CGLS Lake Water Quality datataset geotiff
    """
    if is_local_path(geotiff_url):
        filename = os.path.basename(geotiff_url)
    else:
        filename = posixpath.basename(urlparse(geotiff_url).path)

    # Get the file extension
    _, extension = os.path.splitext(filename)

    # File naming convention in
    # c_gls_<Acronym>_<YYYYMMDDHHmm>_<AREA>_<SENSOR>_<Version>_<tile_index_str>_<subdataset_variable>.<EXTENSION>
    parts = filename.removeprefix(NAMING_PREFIX).removesuffix(extension).split("_")
    parts = list(filter(None, parts))
    acronym, date_str, area, sensor, version, tile_index_str, *subdataset_variable = (
        parts
    )
    subdataset_variable = "_".join(subdataset_variable)

    extension = extension.removeprefix(".")
    return (
        NAMING_PREFIX,
        acronym,
        date_str,
        area,
        sensor,
        version,
        tile_index_str,
        subdataset_variable,
        extension,
    )


def get_dataset_tile_id(geotiff_url: str) -> str:
    """Get the unique tile ID for a CGLS Lake Water Quality dataset.

    Parameters
    ----------
    geotiff_url : str
        File path to one of the measurements of the CGLS Lake Water Quality dataset.

    Returns
    -------
    str
        Unique tile ID for a single CGLS Lake Water Quality dataset
    """
    (
        filename_prefix,
        acronym,
        date_str,
        area,
        sensor,
        version,
        tile_index_str,
        _,
        _,
    ) = parse_geotiff_url(geotiff_url)

    dataset_tile_id = f"{filename_prefix}_{acronym}_{date_str}_{area}_{sensor}_{version}_{tile_index_str}"

    return dataset_tile_id


def parse_dataset_tile_id(dataset_tile_id: str) -> tuple[str]:
    """Get the components of a CGLS Lake Water Quality dataset tile id.


    Parameters
    ----------
    dataset_tile_id : str
        Unique tile ID for a single CGLS Lake Water Quality dataset.

    Returns
    -------
    tuple[str]
        Components of the dataset tile id:
        NAMING_PREFIX, acronym, date_str, area, sensor, version, tile_index_str
    """
    parts = dataset_tile_id.removeprefix(NAMING_PREFIX).split("_")
    parts = list(filter(None, parts))
    acronym, date_str, area, sensor, version, tile_index_str = parts
    return NAMING_PREFIX, acronym, date_str, area, sensor, version, tile_index_str
