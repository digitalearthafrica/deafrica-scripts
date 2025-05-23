"""
Tools for STAC to EO3 translation
Adapted from
https://github.com/opendatacube/odc-tools/blob/develop/apps/dc_tools/odc/apps/dc_tools/_stac.py

Once the following issues are resolved the odc-tools s3-to-dc can be used instead:
https://github.com/opendatacube/odc-tools/issues/622
https://github.com/opendatacube/odc-tools/issues/615
"""

from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from odc.apps.dc_tools._docs import odc_uuid
from odc.apps.dc_tools._stac import (
    DEA_LANDSAT_PRODUCTS,
    TO_BE_HARD_CODED_COLLECTION,
    _check_valid_uuid,
    _find_self_href,
    _geographic_to_projected,
    _get_region_code,
    _get_stac_bands,
    _get_stac_properties_lineage,
    _get_usgs_product_name,
)
from odc.geo.geom import Geometry, box
from pyproj import CRS
from toolz import get_in

Document = Dict[str, Any]


def _stac_product_lookup(
    item: Document,
) -> Tuple[str, Optional[str], str, Optional[str], str]:
    properties = item["properties"]

    dataset_id: str = item["id"]
    dataset_label = item.get("title")

    # Get the simplest version of a product name
    product_name = properties.get("odc:product", None)

    # Try to get a known region_code
    region_code = _get_region_code(properties)

    default_grid = None

    # Maybe this should be the default product_name
    constellation = properties.get("constellation") or properties.get(
        "eo:constellation"
    )
    if constellation is not None:
        constellation = constellation.lower().replace(" ", "-")

    collection = item.get("collection")
    # Special case for USGS Landsat Collection 2
    if collection is not None and collection == "landsat-c2l2-sr":
        product_name = _get_usgs_product_name(properties)

    # It is an ugly hack without interrupting DEAfric sentinel 2
    if constellation is not None and product_name is None:
        if constellation == "sentinel-2" and collection in TO_BE_HARD_CODED_COLLECTION:
            dataset_id = properties.get("sentinel:product_id") or properties.get(
                "s2:granule_id", dataset_id
            )
            if collection == "s2_l2a_c1":
                product_name = "s2_l2a_c1"
            else:
                product_name = "s2_l2a"
            if region_code is None:
                try:
                    # Support v1.0.0 to v1.2.0 of the projection STAC extension
                    # expected format "proj:epsg": 3857
                    native_crs = properties["proj:epsg"]
                    native_crs = CRS.from_epsg(native_crs).to_string()
                except KeyError:
                    # Support v2.0.0 of the projection STAC extension
                    # expected format "proj:code": "EPSG:3857"
                    native_crs = properties["proj:code"]

                # Let's try two options, and throw an exception if we still don't get it
                try:
                    # The 'mgrs' prefix (and STAC extension) started with STAC v1.0.0
                    region_code = (
                        f"{CRS(native_crs).to_authority()[1][-2:]}"
                        f"{properties['mgrs:latitude_band']}"
                        f"{properties['mgrs:grid_square']}"
                    )
                except KeyError:
                    region_code = (
                        f"{CRS(native_crs).to_authority()[1][-2:]}"
                        f"{properties['sentinel:latitude_band']}"
                        f"{properties['sentinel:grid_square']}"
                    )

            default_grid = "g10m"

    # If we still don't have a product name, use collection
    if product_name is None:
        product_name = collection
        if product_name is None:
            raise ValueError("Can't find product name from odc:product or collection.")

    # Product names can't have dashes in them
    product_name = product_name.replace("-", "_")

    if product_name in DEA_LANDSAT_PRODUCTS:
        self_href = _find_self_href(item)
        dataset_label = Path(self_href).stem.replace(".stac-item", "")
        default_grid = "g30m"

    # If the ID is not cold and numerical, assume it can serve a label.
    if (
        not dataset_label
        and not _check_valid_uuid(dataset_id)
        and not dataset_id.isnumeric()
    ):
        dataset_label = dataset_id

    return dataset_id, dataset_label, product_name, region_code, default_grid


def stac_transform(input_stac: Document) -> Document:
    """Takes in a raw STAC 1.0 dictionary and returns an ODC dictionary"""
    # pylint: disable=too-many-locals

    (
        dataset_id,
        dataset_label,
        product_name,
        region_code,
        default_grid,
    ) = _stac_product_lookup(input_stac)

    # Generating UUID for products not having UUID.
    # Checking if provided id is valid UUID.
    # If not valid, creating new deterministic uuid using odc_uuid function
    # based on product_name and product_label.
    # TODO: Verify if this approach to create UUID is valid.
    if _check_valid_uuid(input_stac["id"]):
        deterministic_uuid = input_stac["id"]
    else:
        if product_name in ["s2_l2a"]:
            deterministic_uuid = str(
                odc_uuid("sentinel-2_stac_process", "1.0.0", [dataset_id])
            )
        else:
            deterministic_uuid = str(
                odc_uuid(f"{product_name}_stac_process", "1.0.0", [dataset_id])
            )

    # Check for projection extension properties that are not in the asset fields.
    # Specifically, proj:shape and proj:transform, as these are otherwise
    # fetched in _get_stac_bands.
    properties = input_stac["properties"]
    proj_shape = properties.get("proj:shape")
    proj_transform = properties.get("proj:transform")
    # TODO: handle old STAC that doesn't have grid information here...
    bands, grids, accessories = _get_stac_bands(
        input_stac,
        default_grid,
        proj_shape=proj_shape,
        proj_transform=proj_transform,
    )

    # STAC document may not have top-level proj:shape property
    # use one of the bands as a default
    proj_shape = grids.get("default").get("shape")
    proj_transform = grids.get("default").get("transform")

    stac_properties, lineage = _get_stac_properties_lineage(input_stac)

    try:
        # Support v1.0.0 to v1.2.0 of the projection STAC extension
        # expected format "proj:epsg": 3857
        native_crs = properties["proj:epsg"]
        native_crs = CRS.from_epsg(native_crs).to_string()
    except KeyError:
        # Support v2.0.0 of the projection STAC extension
        # expected format "proj:code": "EPSG:3857"
        native_crs = properties["proj:code"]

    # Transform geometry to the native CRS at an appropriate precision
    geometry = Geometry(input_stac["geometry"], "EPSG:4326")
    if native_crs != "EPSG:4326":
        # Arbitrary precisions, but should be fine
        pixel_size = get_in(["default", "transform", 0], grids, no_default=True)
        precision = 0
        if pixel_size < 0:
            precision = 6

        geometry = _geographic_to_projected(geometry, native_crs, precision)

    if geometry is not None:
        # We have a geometry, but let's make it simple
        geom_type = None
        try:
            geom_type = geometry.geom_type
        except AttributeError:
            geom_type = geometry.type
        if geom_type is not None and geom_type == "MultiPolygon":
            geometry = geometry.convex_hull
    else:
        # Build geometry from the native transform
        min_x = proj_transform[2]
        min_y = proj_transform[5]
        max_x = min_x + proj_transform[0] * proj_shape[0]
        max_y = min_y + proj_transform[4] * proj_shape[1]

        if min_y > max_y:
            min_y, max_y = max_y, min_y

        geometry = box(min_x, min_y, max_x, max_y, native_crs)

    stac_odc = {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "id": deterministic_uuid,
        "crs": native_crs,
        "grids": grids,
        "product": {"name": product_name.lower()},
        "properties": stac_properties,
        "measurements": bands,
        "lineage": {},
        "accessories": accessories,
    }
    if dataset_label:
        stac_odc["label"] = dataset_label

    if region_code:
        stac_odc["properties"]["odc:region_code"] = region_code

    if geometry:
        stac_odc["geometry"] = geometry.json

    if lineage:
        stac_odc["lineage"] = lineage

    return stac_odc
