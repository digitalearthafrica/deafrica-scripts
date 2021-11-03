import json
from calendar import monthrange
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


def _get_path(s3_output_root, out_product, time_str, ext, band=None):
    if band is None:
        return (
            f"{s3_output_root}/{out_product}/{time_str}/{out_product}_{time_str}.{ext}"
        )
    else:
        return f"{s3_output_root}/{out_product}/{time_str}/{out_product}_{time_str}_{band}.{ext}"


def create_mosaic(
    dc: Datacube,
    product: str,
    out_product: str,
    time: Tuple[str, str],
    time_str: str,
    bands: Tuple[str],
    s3_output_root: str,
    split_bands: bool = False,
    resolution: int = 120,
):
    log = setup_logging()
    log.info(f"Creating mosaic for {product} over {time}")

    assets = {}

    if not split_bands:
        all_data = dc.load(
            product=product,
            time=time,
            resolution=(-resolution, resolution),
            dask_chunks={"x": 2048, "y": 2048},
            measurements=bands,
        )
        out_file = _get_path(s3_output_root, out_product, time_str, "tif")
        log.info(f"Writing: {out_file}")
        asset = _save_opinionated_cog(
            all_data.squeeze("time").to_stacked_array("bands", ["x", "y"]),
            out_file,
        )
        assets[bands[0]] = asset
        del all_data
    else:
        log.info("Working on creating multiple tif files")
        for band in bands:
            data = dc.load(
                product=product,
                time=time,
                resolution=(-resolution, resolution),
                dask_chunks={"x": 2048, "y": 2048},
                measurements=[band],
            )
            out_file = _get_path(s3_output_root, out_product, time_str, "tif", band=band)
            log.info(f"Writing: {out_file}")
            asset = _save_opinionated_cog(data[band].squeeze("time"), out_file)
            assets[band] = asset
            del data

    out_stac_file = _get_path(s3_output_root, out_product, time_str, "stac-item.json")
    item = create_stac_item(
        assets[bands[0]].href,
        id=f"{product}_{time_str}",
        assets=assets,
        with_proj=True,
        properties={
            "odc:product": out_product,
            "start_datetime": f"{time[0]}T00:00:00Z",
            "end_datetime": f"{time[1]}T23:59:59Z",
        },
    )
    item.set_self_href(out_stac_file)

    log.info(f"Writing STAC: {out_stac_file}")
    s3_dump(
        data=json.dumps(item.to_dict(), indent=2),
        url=item.self_href,
        ACL="bucket-owner-full-control",
        ContentType="application/json",
    )


@click.command("create-mosaic")
@click.option("--product", type=str, default="gm_ls8_annual")
@click.option("--out-product", type=str, default=None)
@click.option("--time-start", type=str, default="2020")
@click.option("--period", type=str, default="P1Y")
@click.option("--bands", type=str, default="red")
@click.option("--resolution", type=int, default=120)
@click.option(
    "--s3-output-root",
    type=str,
    default="s3://example-bucket/",
)
@click.option("--split-bands", is_flag=True, default=False)
def cli(
    product,
    out_product,
    time_start,
    period,
    bands,
    resolution,
    s3_output_root,
    split_bands,
):
    """
    Create a mosaic of a given product and time period including a STAC item.

    If --split-bands is set, the bands will be split into separate files, and the name will have the band
    name appended to the end.

    An example command is:

        create-mosaic \
            --product gm_ls8_annual \
            --time-start 2013 \
            --period P1Y \
            --bands red,green,blue \
            --resolution 120 \
            --s3-output-root s3://example-bucket/ \
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

    if period not in ["P1Y", "P6M"]:
        print(f"Time period {period} not supported, please use one of P1Y or P6M")

    time_str = f"{time_start}--{period}"
    if period == "P1Y":
        time = (f"{time_start}-01-01", f"{time_start}-12-31")
    elif period == "P6M":
        year, start_month = [int(s) for s in time_start.split("-")]
        end_month = start_month + 6
        end_month_n_days = monthrange(year, end_month)[1]

        time = (
            f"{year}-{start_month:02d}-01",
            f"{year}-{end_month:02d}-{end_month_n_days}",
        )

    if out_product is None:
        out_product = f"{product}_{resolution}"

    create_mosaic(
        dc,
        product,
        out_product,
        time,
        time_str,
        bands,
        s3_output_root.rstrip("/"),
        split_bands,
        resolution,
    )
