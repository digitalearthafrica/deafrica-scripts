"""
Build S3 iterators using odc-tools
and index datasets found into RDS

Adapted from
https://github.com/opendatacube/odc-tools/blob/develop/apps/dc_tools/odc/apps/dc_tools/_stac.py
https://github.com/opendatacube/odc-tools/blob/develop/apps/dc_tools/odc/apps/dc_tools/s3_to_dc.py

to support  https://stac-extensions.github.io/projection/v2.0.0/schema.json
"""

import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import click
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from odc.aio import S3Fetcher, s3_find_glob
from odc.apps.dc_tools._docs import odc_uuid, parse_doc_stream
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
from odc.apps.dc_tools.utils import (
    IndexingException,
    SkippedException,
    allow_unsafe,
    archive_less_mature,
    fail_on_missing_lineage,
    index_update_dataset,
    no_sign_request,
    publish_action,
    request_payer,
    skip_check,
    skip_lineage,
    statsd_gauge_reporting,
    statsd_setting,
    transform_stac,
    update_flag,
    update_if_exists_flag,
    verify_lineage,
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


def doc_error(uri, doc):
    """Log the internal errors parsing docs"""
    logging.exception("Failed to parse doc at %s", uri)


def dump_to_odc(
    document_stream,
    dc: Datacube,
    products: list,
    transform=None,
    update=False,
    update_if_exists=False,
    allow_unsafe=False,
    archive_less_mature=None,
    publish_action=None,
    **kwargs,
) -> Tuple[int, int, int]:
    doc2ds = Doc2Dataset(dc.index, products=products, **kwargs)

    ds_added = 0
    ds_failed = 0
    ds_skipped = 0
    uris_docs = parse_doc_stream(
        ((doc.url, doc.data) for doc in document_stream),
        on_error=doc_error,
    )

    found_docs = False
    for uri, metadata in uris_docs:
        found_docs = True
        stac_doc = None
        if transform:
            stac_doc = metadata
            metadata = stac_transform(metadata)
        try:
            index_update_dataset(
                metadata,
                uri,
                dc,
                doc2ds,
                update=update,
                update_if_exists=update_if_exists,
                allow_unsafe=allow_unsafe,
                archive_less_mature=archive_less_mature,
                publish_action=publish_action,
                stac_doc=stac_doc,
            )
            ds_added += 1
        except IndexingException:
            logging.exception("Failed to index dataset %s", uri)
            ds_failed += 1
        except SkippedException:
            ds_skipped += 1
    if not found_docs:
        raise click.ClickException("Doc stream was empty")

    return ds_added, ds_failed, ds_skipped


@click.command("s3-to-dc-v2")
@click.option(
    "--log",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="WARNING",
    show_default=True,
    help="control the log level, e.g., --log=error",
)
@skip_lineage
@fail_on_missing_lineage
@verify_lineage
@transform_stac
@update_flag
@update_if_exists_flag
@allow_unsafe
@skip_check
@no_sign_request
@statsd_setting
@request_payer
@archive_less_mature
@publish_action
@click.argument("uris", nargs=-1)
@click.argument("product", type=str, nargs=1, required=False)
def cli(
    log,
    skip_lineage,
    fail_on_missing_lineage,
    verify_lineage,
    stac,
    update,
    update_if_exists,
    allow_unsafe,
    skip_check,
    no_sign_request,
    statsd_setting,
    request_payer,
    archive_less_mature,
    publish_action,
    uris,
    product,
):
    """
    Iterate through files in an S3 bucket and add them to datacube.

    File uris can be provided as a glob, or as a list of absolute URLs.
    If more than one uri is given, all will be treated as absolute URLs.

    Product is optional; if one is provided, it must match all datasets.
    Can provide a single product name or a space separated list of multiple products
    (formatted as a single string).
    """
    log_level = getattr(logging, log.upper())
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s: %(levelname)s: %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S",
    )

    opts = {}
    if request_payer:
        opts["RequestPayer"] = "requester"

    dc = Datacube()

    # if it's a uri, a product wasn't provided, and 'product' is actually another uri
    if product.startswith("s3://"):
        candidate_products = []
        uris += (product,)
    else:
        # Check datacube connection and products
        candidate_products = product.split()
        odc_products = dc.list_products().name.values

        odc_products = set(odc_products)
        if not set(candidate_products).issubset(odc_products):
            missing_products = list(set(candidate_products) - odc_products)
            print(
                f"Error: Requested Product/s {', '.join(missing_products)} "
                f"{'is' if len(missing_products) == 1 else 'are'} "
                "not present in the ODC Database",
                file=sys.stderr,
            )
            sys.exit(1)

    is_glob = True
    # we assume the uri to be an absolute URL if it contains no wildcards
    # or if there are multiple uri values provided
    if (len(uris) > 1) or ("*" not in uris[0]):
        is_glob = False
        for url in uris:
            if "*" in url:
                logging.warning(
                    "A list of uris is assumed to include only absolute URLs. "
                    "Any wildcard characters will be escaped."
                )

    # Get a generator from supplied S3 Uri for candidate documents
    fetcher = S3Fetcher(aws_unsigned=no_sign_request)
    # Grab the URL from the resulting S3 item
    if is_glob:
        document_stream = (
            url.url
            for url in s3_find_glob(uris[0], skip_check=skip_check, s3=fetcher, **opts)
        )
    else:
        # if working with absolute URLs, no need for all the globbing logic
        document_stream = uris

    added, failed, skipped = dump_to_odc(
        fetcher(document_stream),
        dc,
        candidate_products,
        skip_lineage=skip_lineage,
        fail_on_missing_lineage=fail_on_missing_lineage,
        verify_lineage=verify_lineage,
        transform=stac,
        update=update,
        update_if_exists=update_if_exists,
        allow_unsafe=allow_unsafe,
        archive_less_mature=archive_less_mature,
        publish_action=publish_action,
    )

    print(
        f"Added {added} datasets, skipped {skipped} datasets and failed {failed} datasets."
    )
    if statsd_setting:
        statsd_gauge_reporting(added, ["app:s3_to_dc", "action:added"], statsd_setting)
        statsd_gauge_reporting(
            skipped, ["app:s3_to_dc", "action:skipped"], statsd_setting
        )
        statsd_gauge_reporting(
            failed, ["app:s3_to_dc", "action:failed"], statsd_setting
        )

    if failed > 0:
        sys.exit(failed)


if __name__ == "__main__":
    cli()
