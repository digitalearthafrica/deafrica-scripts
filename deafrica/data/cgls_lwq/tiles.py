"""
This module contains functions to handle tile management
for the CGLS Lake Water Quality products.
"""

import os
import posixpath
import re

import geopandas as gpd
from odc.geo import XY, Resolution
from odc.geo.geom import Geometry
from odc.geo.gridspec import GridSpec

from deafrica.io import is_local_path
from deafrica.utils import AFRICA_EXTENT_URL


def get_tile_index_str_tuple(string_: str) -> tuple[str]:
    """Get the tile index in the string
    format "{x:03d}","{y:03d}" from a string.

    Parameters
    ----------
    string_ : str
        String to search the tile index from.

    Returns
    -------
    tuple[str]
        Tile index in the string format "{x:03d}","{y:03d}" .
    """
    x_pattern = re.compile(r"x\d{3}")
    y_pattern = re.compile(r"y\d{3}")

    tile_index_x_str = re.search(x_pattern, string_).group(0)
    tile_index_y_str = re.search(y_pattern, string_).group(0)

    return tile_index_x_str, tile_index_y_str


def get_tile_index_int_tuple(string_: str) -> tuple[int, int]:
    """
    Get the tile index in the format (x,y) from a string
    where x and y are integers.

    Parameters
    ----------
    string_ : str
        String to search the tile index from.

    Returns
    -------
    tuple[int, int]
        Tile index in the format (x,y) where x and y are integers.
    """

    tile_index_x_str, tile_index_y_str = get_tile_index_str_tuple(string_)

    tile_index_x = int(tile_index_x_str.lstrip("x"))
    tile_index_y = int(tile_index_y_str.lstrip("y"))

    tile_index = (tile_index_x, tile_index_y)

    return tile_index


def get_tile_index_str(tile_index_tuple: tuple[int, int]) -> str:
    """
    Convert a tile index tuple (x,y) into the string format "{x:03d}","{y:03d}" .

    Parameters
    ----------
    tile_index_tuple : tuple[int, int]
        Tile index tuple to convert to string.

    Returns
    -------
    str
        Tile index in string format "{x:03d}","{y:03d}" .
    """

    tile_index_x, tile_index_y = tile_index_tuple

    tile_index_str = f"x{tile_index_x:03d}y{tile_index_y:03d}"

    return tile_index_str


def get_tile_index_tuple_from_filename(file_path: str) -> tuple[int, int]:
    """
    Search for a tile index in the base name of a file.

    Parameters
    ----------
    file_path : str
        File path to search tile index in.

    Returns
    -------
    tuple[int, int]
        Found tile index (x,y).
    """
    if is_local_path(file_path):
        file_name = os.path.splitext(os.path.basename(file_path))[0]
    else:
        file_name = os.path.splitext(posixpath.basename(file_path))[0]

    tile_id = get_tile_index_int_tuple(file_name)

    return tile_id


def get_africa_tiles(grid_res: int | float) -> list:
    """
    Get tiles over Africa extent.

    Parameters
    ----------
    grid_res : int | float
        Grid resolution in projected crs (EPSG:6933).

    Returns
    -------
    list
        List of tiles, each item contains the tile index and the tile geobox.
    """
    if grid_res == 300:
        multiplier = 10
    elif grid_res == 100:
        multiplier = 10 * 3
    else:
        NotImplementedError(f"gridspec for resolution {grid_res} not implemented")

    gridspec = GridSpec(
        crs="EPSG:6933",
        tile_shape=XY(y=320 * multiplier, x=320 * multiplier),
        resolution=Resolution(y=-grid_res, x=grid_res),
        origin=XY(y=-7392000, x=-17376000),
    )

    # Get the tiles over Africa
    africa_extent = gpd.read_file(AFRICA_EXTENT_URL).to_crs(gridspec.crs)
    africa_extent_geom = Geometry(
        geom=africa_extent.iloc[0].geometry, crs=africa_extent.crs
    )
    tiles = list(gridspec.tiles_from_geopolygon(africa_extent_geom))

    return tiles
