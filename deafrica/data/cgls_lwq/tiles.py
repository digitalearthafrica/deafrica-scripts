"""
This module contains functions to handle tile management
for the CGLS Lake Water Quality products.
"""

import geopandas as gpd
from odc.dscache._utils import to_tile_shape
from odc.geo import CRS, XY, Resolution
from odc.geo.geom import Geometry
from odc.geo.gridspec import GridSpec

from deafrica.utils import AFRICA_EXTENT_URL


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

    # Each tile should be 960_000m x 960_000m in size
    tile_shape = to_tile_shape((960_000.0, 960_000.0), grid_res)

    gridspec = GridSpec(
        crs=CRS("EPSG:6933"),
        #
        tile_shape=tile_shape,
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


def get_region_code(tile_id: tuple[int, int], sep: str = "") -> str:
    """
    Get the region code for a tile from its tile ID in the format
    format "x{x:03d}{sep}y{y:03d}".

    Parameters
    ----------
    tile_id : tuple[int, int]
        Tile ID for the tile.
    sep : str, optional
        Seperator between the x and y parts of the region code, by
        default ""

    Returns
    -------
    str
        Region code for the input tile ID.
    """
    x, y = tile_id
    region_code_format = "x{x:03d}{sep}y{y:03d}"
    region_code = region_code_format.format(x=x, y=y, sep=sep)
    return region_code
