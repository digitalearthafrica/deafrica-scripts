import os
import posixpath

from deafrica.io import is_local_path


def parse_geotiff_url(geotiff_url: str) -> tuple[str]:
    """
    Get the filename components of an ESA WorldCereal
    AEZ-based GeoTIFF url.

    Parameters
    ----------
    geotiff_url : str
        ESA WorldCereal AEZ-based GeoTIFF file path.

    Returns
    -------
    tuple[str]
        Filename components of an ESA WorldCereal
        AEZ-based GeoTIFF file i.e aez_id, season,
        product, startdate, enddate, band_name.
    """
    if is_local_path(geotiff_url):
        filename = os.path.basename(geotiff_url)
    else:
        filename = posixpath.basename(geotiff_url)

    # File naming convention in
    # {AEZ_id}_{season}_{product}_{startdate}_{enddate}_{classification|confidence}.tif
    name, extension = os.path.splitext(filename)
    aez_id, season, product, startdate, enddate, band_name = name.split("_")
    return aez_id, season, product, startdate, enddate, band_name


def get_dataset_tile_id(geotiff_url: str) -> str:
    """Get the unique tile ID for an ESA WorldCereal dataset.

    Parameters
    ----------
    geotiff_url : str
        File path to one of the measurements of the ESA WorldCereal dataset.

    Returns
    -------
    str
        Unique tile ID for a single ESA WorldCereal dataset.
    """
    aez_id, season, product, startdate, enddate, band_name = parse_geotiff_url(
        geotiff_url
    )

    dataset_tile_id = f"{aez_id}_{season}_{product}_{startdate}_{enddate}"

    return dataset_tile_id


def parse_dataset_tile_id(dataset_tile_id: str) -> tuple[str]:
    """Get the components of an ESA WorldCereal dataset tile id.

    Parameters
    ----------
    dataset_tile_id : str
        Unique tile ID for a single ESA WorldCereal dataset.

    Returns
    -------
    tuple[str]
        Components of the dataset tile id:
        aez_id, season, product, startdate, enddate
    """
    aez_id, season, product, startdate, enddate = dataset_tile_id.split("_")
    return aez_id, season, product, startdate, enddate
