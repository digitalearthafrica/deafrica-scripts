"""
Functions to parse info from Copernicus Global Land Service -
Lake Water Quality NetCDF files.
"""

import logging
import os
import posixpath
import re
from urllib.parse import urlparse

import rasterio
import requests
import rioxarray
import xarray as xr

from deafrica.data.cgls_lwq.constants import NAMING_PREFIX
from deafrica.io import is_local_path

log = logging.getLogger(__name__)


def parse_netcdf_url(netcdf_url: str) -> tuple[str]:
    """
    Get the filename components of a CGLS Lake Water Quality
    netcdf url.

    Parameters
    ----------
    netcdf_url : str
        CGLS Lake Water Quality netcdf url

    Returns
    -------
    tuple[str]
        Filename components of a CGLS Lake Water Quality netcdf url.
    """
    if is_local_path(netcdf_url):
        filename = os.path.basename(netcdf_url)
    else:
        filename = posixpath.basename(netcdf_url)

    # Get the file extension
    _, extension = os.path.splitext(filename)

    # File naming convention in
    # c_gls_<Acronym>_<YYYYMMDDHHmm>_<AREA>_<SENSOR>_<Version>.<EXTENSION>
    parts = filename.removeprefix(NAMING_PREFIX).removesuffix(extension).split("_")
    parts = list(filter(None, parts))
    acronym, date_str, area, sensor, version = parts
    extension = extension.removeprefix(".")

    return NAMING_PREFIX, acronym, date_str, area, sensor, version, extension


def parse_netcdf_subdatasets_uri(netcdf_uri: str) -> tuple[str]:
    """
    Parse a CGLS Lake Water Quality netcdf subdaset URI to get the driver,
    GDAL VFS prefix, parent netcdf url and the name of the subdatset.

    Parameters
    ----------
    netcdf_uri : str
        CGLS Lake Water Quality netcdf subdaset URI

    Returns
    -------
    tuple[str]
        Driver, GDAL VFS prefix, parent netcdf url and the name of the subdatset
    """
    # subdaset uri format driver:"filename":subdataset_variable
    # or driver:/vsiprefix/URL[:subdataset_variable]
    driver = netcdf_uri.split(":")[0]
    assert driver.lower() == "netcdf"

    subdataset_variable = netcdf_uri.split(":")[-1]

    netcdf_url = netcdf_uri.removeprefix(f"{driver}:").removesuffix(
        f":{subdataset_variable}"
    )

    matches = re.search(r"^/vsi[^/]+/", netcdf_url)
    if matches is None:
        vsiprefix = ""
    else:
        vsiprefix = matches.group()  # .strip("/")

    netcdf_url = netcdf_url.replace(vsiprefix, "")
    return driver, vsiprefix, netcdf_url, subdataset_variable


def get_netcdf_subdataset_variable(netcdf_uri: str) -> tuple[str]:
    """Get the name of a CGLS Lake Water Quality netcdf subdaset from
    its URI.

    Parameters
    ----------
    netcdf_uri : str
        CGLS Lake Water Quality netcdf subdaset URI.

    Returns
    -------
    tuple[str]
        Name of the subdataset.
    """
    _, _, _, subdataset_variable = parse_netcdf_subdatasets_uri(netcdf_uri)
    return subdataset_variable


def get_netcdf_subdatasets_uris(netcdf_url: str) -> dict[str, str]:
    """Get a dictionary mapping a subdatset's name to its URI for all
    subddatasets from a CGLS Lake Water Quality netcdf file

    Parameters
    ----------
    netcdf_url : str
        URL to the GLS Lake Water Quality netcdf file

    Returns
    -------
    dict[str, str]
        Mapping of name to URI for all subdatasets in the CGLS Lake Water Quality netcdf file
    """

    with rasterio.open(netcdf_url, "r") as src:
        subdatasets = src.subdatasets

    netcdf_subdatasets_uris = {
        get_netcdf_subdataset_variable(i): i for i in subdatasets
    }

    return netcdf_subdatasets_uris


def get_netcdf_subdatasets_names(netcdf_url: str) -> list[str]:
    """Get a list of all the subdatasets names in a
    CGLS Lake Water Quality netcdf file

    Parameters
    ----------
    netcdf_url : str
        URL to the GLS Lake Water Quality netcdf file

    Returns
    -------
    list[str]
        List of all the subdatasets names in the
        CGLS Lake Water Quality netcdf file
    """
    netcdf_subdatasets_names = list(get_netcdf_subdatasets_uris(netcdf_url).keys())
    return netcdf_subdatasets_names


def get_netcdf_urls_from_manifest(manifest_file_url: str) -> list[str]:
    # Get all the urls from the manifest file
    r = requests.get(manifest_file_url)
    all_netcdf_urls = [i.strip() for i in r.text.splitlines()]
    all_netcdf_urls = sorted(all_netcdf_urls, key=lambda url: posixpath.basename(url))

    # Filter to remove duplicates
    grouped_urls = {}
    for url in all_netcdf_urls:
        key = posixpath.basename(url)
        existing_value = grouped_urls.get(key, None)
        if existing_value is not None:
            if posixpath.basename(url) == posixpath.basename(existing_value):
                if "//" in urlparse(existing_value).path:
                    grouped_urls[key] = url
                else:
                    pass
            else:
                NotImplementedError()
        else:
            grouped_urls[key] = url

    all_netcdf_urls = list(grouped_urls.values())

    check = [i for i in all_netcdf_urls if "//" in urlparse(i).path]
    assert len(check) == 0

    return all_netcdf_urls


def read_netcdf_url(netcdf_url: str, max_retries: int = 5) -> xr.Dataset | xr.DataArray:
    """
    Read a netcdf url into an xarray object with a retry step.

    Parameters
    ----------
    netcdf_url : str
        File path or URI to the netcdf file to read
    max_retries : int, optional
        Maximum number of times to try reading the file, by default 5

    Returns
    -------
    xr.Dataset | xr.DataArray
        Data from the netcdf file.
    """
    attempt = 1
    while True:
        try:
            ds = rioxarray.open_rasterio(netcdf_url)
        except Exception as error:
            log.error(f"Read attempt {attempt} for {netcdf_url} failed")
            if attempt == max_retries:
                log.error("Reached max retry attempts")
                raise error
            else:
                attempt += 1
        else:
            return ds
