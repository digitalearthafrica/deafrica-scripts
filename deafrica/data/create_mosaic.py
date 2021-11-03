import json
from typing import Tuple

import click
import pystac
from datacube import Datacube
from deafrica.utils import setup_logging
from odc.algo import save_cog
from odc.aws import s3_dump
from rio_stac import create_stac_item


def _save_opinionated_cog(data, out_file):
    cog = save_cog(
        data,
        out_file,
        blocksize=1024,
        overview_resampling="average",
        NUM_THREADS="ALL_CPUS",
        bigtiff="YES",
        SPARSE_OK=True,
        ACL="bucket-owner-full-control",
    )
    cog.compute()
    cog.release()

    return pystac.Asset(media_type=pystac.MediaType.COG, href=out_file, roles=["data"])


def create_mosaic(
    dc: Datacube,
    product: str,
    out_product: str,
    year: str,
    bands: Tuple[str],
    s3_output_file: str,
    split_bands: bool = False,
    resolution: int = 120,
):
    log = setup_logging()
    log.info(f"Creating mosaic for {product} over {year}")

    assets = {}
    data = dc.load(
        product=product,
        time=year,
        resolution=(-resolution, resolution),
        dask_chunks={"x": 2048, "y": 2048},
        measurements=bands,
    )

    if not split_bands:
        log.info(f"Writing: {s3_output_file}")
        asset = _save_opinionated_cog(
            data.squeeze("time").to_stacked_array("bands", ["x", "y"]),
            s3_output_file,
        )
        assets[bands[0]] = asset
    else:
        log.info("Working on creating multiple tif files")
        for band in bands:
            out_file = s3_output_file.replace(".tif", f"_{band}.tif")
            log.info(f"Writing: {out_file}")
            asset = _save_opinionated_cog(data[band].squeeze("time"), out_file)
            assets[band] = asset

    out_stac_file = s3_output_file.replace(".tif", ".stac-item.json")
    log.info("Creating STAC item")
    item = create_stac_item(
        assets[bands[0]].href,
        id=f"{product}_{year}",
        assets=assets,
        with_proj=True,
        properties={
            "odc:product": out_product,
            "start_datetime": f"{year}-01-01T00:00:00Z",
            "end_datetime": f"{year}-12-31T23:59:59Z",
        },
    )
    item.set_self_href(out_stac_file)

    log.info(f"Write STAC: {out_stac_file}")
    s3_dump(
        data=json.dumps(item.to_dict(), indent=2),
        url=item.self_href,
        ACL="bucket-owner-full-control",
        ContentType="application/json",
    )


@click.command("create-mosaic")
@click.option("--product", type=str, default="gm_ls8_annual")
@click.option("--out-product", type=str, default=None)
@click.option("--year", type=str, default="2020")
@click.option("--bands", type=str, default="red")
@click.option("--resolution", type=int, default=120)
@click.option(
    "--s3-output-file",
    type=str,
    default="s3://example-bucket/example-path/example_file_name.tif",
)
@click.option("--split-bands", is_flag=True, default=False)
def cli(product, out_product, year, bands, resolution, s3_output_file, split_bands):
    """
    Create a mosaic of a given product and time period including a STAC item.

    If --split-bands is set, the bands will be split into separate files, and the name will have the band
    name appended to the end.

    An example command is:

        create-mosaic \
            --product gm_ls8_annual \
            --year 2013 \
            --bands red,green,blue \
            --resolution 120 \
            --s3-output-file s3://deafrica-data-dev-af/alex-test-mosaics/test_120_split/test_120m.tif \
            --split-bands
    """
    dc = Datacube()

    bands = bands.split(",")
    if not len(bands) > 0:
        print("Please select at least one band")
        exit(1)

    if not dc.index.products.get_by_name(product):
        print(f"Product {product} not found")
        exit(1)

    if out_product is None:
        out_product = f"{product}_{resolution}"

    create_mosaic(
        dc, product, out_product, year, bands, s3_output_file, split_bands, resolution
    )
