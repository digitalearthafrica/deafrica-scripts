#!/usr/bin/env python3

import datetime
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Tuple

import boto3
import click
import rasterio
from odc.index import odc_uuid
from osgeo import gdal
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from ruamel.yaml import YAML
from deafrica.utils import setup_logging
from logging import Logger

NS = [
    "N40",
    "N35",
    "N30",
    "N25",
    "N20",
    "N15",
    "N10",
    "N05",
    "N00",
    "S05",
    "S10",
    "S15",
    "S20",
    "S25",
    "S30",
]

EW = [
    "W020",
    "W015",
    "W010",
    "W005",
    "E000",
    "E005",
    "E010",
    "E015",
    "E020",
    "E025",
    "E030",
    "E035",
    "E040",
    "E045",
    "E050",
]


def make_directories(directories: Tuple[Path, Path], log):
    for directory in directories:
        if not directory.exists():
            log.info(f"Creating directory {directory}")
            directory.mkdir(parents=True)


def delete_directories(directories: Tuple[Path, Path], log):
    log.info("Deleting directories...")
    for directory in directories:
        if directory.exists():
            log.info(f"Deleting directory {directory}")
            shutil.rmtree(directory)


def download_files(workdir, year, tile, log):
    if int(year) > 2010:
        filename = f"{tile}_{year[-2:]}_MOS_F02DAR.tar.gz"
    else:
        filename = f"{tile}_{year[-2:]}_MOS.tar.gz"

    log.info(f"Downloading file: {filename}")
    if int(year) > 2010:
        ftp_location = f"ftp://ftp.eorc.jaxa.jp/pub/ALOS-2/ext1/PALSAR-2_MSC/25m_MSC/{year}/{filename}"
    elif int(year) < 2000:
        ftp_location = (
            f"ftp://ftp.eorc.jaxa.jp/pub/ALOS/ext1/JERS-1_MSC/25m_MSC/{year}/{filename}"
        )
    else:
        ftp_location = (
            f"ftp://ftp.eorc.jaxa.jp/pub/ALOS/ext1/PALSAR_MSC/25m_MSC/{year}/{filename}"
        )
    tar_file = workdir / filename

    try:
        if not tar_file.exists():
            log.info(f"Downloading file: {ftp_location}")
            subprocess.check_call(["wget", "-q", ftp_location], cwd=workdir)
        else:
            log.info("Skipping download, file already exists")
        log.info("Untarring file")
        subprocess.check_call(["tar", "-xf", filename], cwd=workdir)
    except subprocess.CalledProcessError:
        log.exception("File failed to download...")


def combine_cog(PATH, OUTPATH, tile, year, log):
    log.info("Combining GeoTIFFs")
    if int(year) > 2000:
        bands = ["HH", "HV", "linci", "date", "mask"]
    else:
        bands = ["HH", "linci", "date", "mask"]
    output_cogs = []

    gtiff_abs_path = os.path.abspath(PATH)
    outtiff_abs_path = os.path.abspath(OUTPATH)

    for band in bands:
        # Find all the files
        all_files = []
        for path, _, files in os.walk(gtiff_abs_path):
            for fname in files:
                if int(year) > 2010:
                    if "_{}_".format(band) in fname and not fname.endswith(".hdr"):
                        in_filename = os.path.join(path, fname)
                        all_files.append(in_filename)
                else:
                    if "_{}".format(band) in fname and not fname.endswith(".hdr"):
                        in_filename = os.path.join(path, fname)
                        all_files.append(in_filename)

        # Create the VRT
        log.info("Building VRT for {} with {} files found".format(band, len(all_files)))
        vrt_path = os.path.join(gtiff_abs_path, "{}.vrt".format(band))
        if int(year) > 2010:
            cog_filename = os.path.join(
                outtiff_abs_path, "{}_{}_sl_{}_F02DAR.tif".format(tile, year[-2:], band)
            )
        else:
            cog_filename = os.path.join(
                outtiff_abs_path, "{}_{}_sl_{}.tif".format(tile, year[-2:], band)
            )
        vrt_options = gdal.BuildVRTOptions()
        gdal.BuildVRT(vrt_path, all_files, options=vrt_options)

        # Default to nearest resampling
        resampling = "nearest"
        if band in ["HH", "HV", "linci"]:
            resampling = "average"

        cog_translate(
            vrt_path,
            cog_filename,
            cog_profiles.get("deflate"),
            config={"GDAL_TIFF_OVR_BLOCKSIZE": "512"},
            overview_level=5,
            overview_resampling=resampling,
            nodata=0,
        )

        output_cogs.append(cog_filename)

    # Return the list of written files
    return output_cogs


def fix_values(num):
    # We do this to get rid of tiny floating point versions of zero and add a little number to remove negative zero
    return round(num + 0.000000001, 3)


def get_ref_points(bounds):
    return {
        "ll": {"x": fix_values(bounds[0]), "y": fix_values(bounds[1])},
        "lr": {"x": fix_values(bounds[2]), "y": fix_values(bounds[1])},
        "ul": {"x": fix_values(bounds[0]), "y": fix_values(bounds[3])},
        "ur": {"x": fix_values(bounds[2]), "y": fix_values(bounds[3])},
    }


def get_coords(bounds):
    return {
        "ll": {"lat": fix_values(bounds[1]), "lon": fix_values(bounds[0])},
        "lr": {"lat": fix_values(bounds[1]), "lon": fix_values(bounds[2])},
        "ul": {"lat": fix_values(bounds[3]), "lon": fix_values(bounds[0])},
        "ur": {"lat": fix_values(bounds[3]), "lon": fix_values(bounds[2])},
    }


def write_yaml(outdir, year, tile, log):
    log.info("Writing yaml.")
    yaml_filename = os.path.join(outdir, "{}_{}.yaml".format(tile, year))
    if int(year) > 2010:
        datasetpath = os.path.join(
            outdir, "{}_{}_sl_HH_F02DAR.tif".format(tile, year[-2:])
        )
    else:
        datasetpath = os.path.join(outdir, "{}_{}_sl_HH.tif".format(tile, year[-2:]))
    dataset = rasterio.open(datasetpath)
    bounds = dataset.bounds
    geo_ref_points = get_ref_points(bounds)
    coords = get_coords(bounds)
    creation_date = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
    if int(year) > 2010:
        hhpath = "{}_{}_sl_HH_F02DAR.tif".format(tile, year[-2:])
        hvpath = "{}_{}_sl_HV_F02DAR.tif".format(tile, year[-2:])
        lincipath = "{}_{}_sl_linci_F02DAR.tif".format(tile, year[-2:])
        maskpath = "{}_{}_sl_mask_F02DAR.tif".format(tile, year[-2:])
        datepath = "{}_{}_sl_date_F02DAR.tif".format(tile, year[-2:])
        launch_date = "2014-05-24"
        shortname = "alos"
    else:
        hhpath = "{}_{}_sl_HH.tif".format(tile, year[-2:])
        hvpath = "{}_{}_sl_HV.tif".format(tile, year[-2:])
        lincipath = "{}_{}_sl_linci.tif".format(tile, year[-2:])
        maskpath = "{}_{}_sl_mask.tif".format(tile, year[-2:])
        datepath = "{}_{}_sl_date.tif".format(tile, year[-2:])
        if int(year) > 2000:
            launch_date = "2006-01-24"
            shortname = "alos"
        else:
            launch_date = "1992-02-11"
            shortname = "jers"
    if shortname == "alos":
        platform = "ALOS/ALOS-2"
        instrument = "PALSAR/PALSAR-2"
        cf = "83.0 dB"
        bandpaths = {
            "hh": {"path": hhpath},
            "hv": {"path": hvpath},
            "linci": {"path": lincipath},
            "mask": {"path": maskpath},
            "date": {"path": datepath},
        }
    else:
        platform = "JERS-1"
        instrument = "SAR"
        cf = "84.66 dB"
        bandpaths = {
            "hh": {"path": hhpath},
            "linci": {"path": lincipath},
            "mask": {"path": maskpath},
            "date": {"path": datepath},
        }
    metadata_doc = {
        "id": str(odc_uuid(shortname, "1", [], year=year, tile=tile)),
        "creation_dt": creation_date,
        "product_type": "gamma0",
        "platform": {"code": platform},
        "instrument": {"name": instrument},
        "format": {"name": "GeoTIFF"},
        "extent": {
            "coord": coords,
            "from_dt": "{}-01-01T00:00:01".format(year),
            "center_dt": "{}-06-15T11:00:00".format(year),
            "to_dt": "{}-12-31T23:59:59".format(year),
        },
        "grid_spatial": {
            "projection": {
                "geo_ref_points": geo_ref_points,
                "spatial_reference": "EPSG:4326",
            }
        },
        "image": {
            "bands": bandpaths,
        },
        "lineage": {"source_datasets": {}},
        "property": {
            "launchdate": launch_date,
            "cf": cf,
        },
    }

    with open(yaml_filename, "w") as f:
        yaml = YAML(typ="safe", pure=False)
        yaml.default_flow_style = False
        yaml.dump(metadata_doc, f)

    return yaml_filename


def upload_to_s3(s3_bucket, path, files, log):
    log.info("Commencing S3 upload")
    s3r = boto3.resource("s3")
    if s3_bucket:
        log.info("Uploading to {}".format(s3_bucket))
        # Upload data
        for out_file in files:
            data = open(out_file, "rb")
            key = "{}/{}".format(path, os.path.basename(out_file))
            log.info("Uploading file {} to S3://{}/{}".format(out_file, s3_bucket, key))
            s3r.Bucket(s3_bucket).put_object(
                Key=key, Body=data, acl="bucket-owner-full-control"
            )
    else:
        log.warning("Not uploading to S3, because the bucket isn't set.")


def run_one(tile_string: str, workdir: Path, s3_destination: str, log: Logger):
    year = tile_string.split("/")[0]
    tile = tile_string.split("/")[1]

    path = tile_string

    outdir = workdir / "out"

    try:
        log.info(f"Starting up process for tile {tile_string}")
        make_directories([workdir, outdir], log)
        download_files(workdir, year, tile, log)
        list_of_cogs = combine_cog(workdir, outdir, tile, year, log)
        metadata_file = write_yaml(outdir, year, tile, log)
        upload_to_s3(outdir, s3_destination, path, list_of_cogs + [metadata_file], log)
        delete_directories([workdir, outdir], log)
    except Exception:
        log.exception(f"Job failed for tile {tile_string}")


@click.command("download-alos-palsar")
@click.option(
    "--tile-string",
    "-t",
    required=True,
    help="The tile to process, in the form YYYY/tile, like 2020/",
)
@click.option(
    "--workdir",
    "-w",
    default="/tmp/download",
    help="The directory to download files to",
)
@click.option("--s3-bucket", "-s", required=False, help="The S3 bucket to upload to")
@click.option("--s3-path", "-p", required=False, help="The S3 path to upload to")
def cli(tile_string, workdir, s3_bucket, s3_path):
    """
    Example command:

    download-alos-palsar --tile-string 2020/N10E010 -w /tmp/download -s example-bucket -p alos_palsar_mosaic
    """
    log = setup_logging()

    s3_destination = s3_bucket.rstrip("/") + "/" + s3_path.rstrip("/")

    run_one(tile_string, Path(workdir), s3_destination, log)


@click.command("alos-palsar-dump-tiles")
@click.option("--years", default="2020", help="The year to dump the strings of.")
def dump_tiles(years):
    def _get_tiles():
        for year in years.split(","):
            for ns in NS:
                for ew in EW:
                    yield "{}/{}".format(year, ns + ew)

    json.dump(list(_get_tiles()), sys.stdout)


if __name__ == "__main__":
    cli()
