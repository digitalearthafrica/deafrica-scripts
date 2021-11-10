#!/usr/bin/env python3

import json
import os
import shutil
import subprocess
import sys
from logging import Logger
from pathlib import Path
from typing import Tuple

import click
import pystac
from deafrica.utils import setup_logging
from odc.aws import s3_dump, s3_head_object
from odc.index import odc_uuid
from osgeo import gdal
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from rio_stac import create_stac_item

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
    except subprocess.CalledProcessError:
        log.warning("File failed to download... skipping")
        exit(0)

    log.info("Untarring file")
    subprocess.check_call(["tar", "-xf", filename, "--no-same-owner"], cwd=workdir)


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
            config={"GDAL_TIFF_OVR_BLOCKSIZE": "512", "CHECK_DISK_FREE_SPACE": False},
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


def write_stac(
    s3_destination: str, file_path: str, file_key: str, year: str, log: Logger
) -> str:
    region_code = file_key.split("_")[0]
    stac_href = f"s3://{s3_destination}/{file_key}.stac-item.json"
    log.info(f"Creating STAC file in memory, targeting here: {stac_href}")

    if int(year) > 2010:
        hhpath = f"{file_key}_sl_HH_F02DAR.tif"
        hvpath = f"{file_key}_sl_HV_F02DAR.tif"
        lincipath = f"{file_key}_sl_linci_F02DAR.tif"
        maskpath = f"{file_key}_sl_mask_F02DAR.tif"
        datepath = f"{file_key}_sl_date_F02DAR.tif"
        launch_date = "2014-05-24"
        shortname = "alos"
    else:
        hhpath = f"{file_key}_sl_HH.tif"
        hvpath = f"{file_key}_sl_HV.tif"
        lincipath = f"{file_key}_sl_linci.tif"
        maskpath = f"{file_key}_sl_mask.tif"
        datepath = f"{file_key}_sl_date.tif"
        if int(year) > 2000:
            launch_date = "2006-01-24"
            shortname = "alos"
        else:
            launch_date = "1992-02-11"
            shortname = "jers"
    if shortname == "alos":
        product_name = "alos_palsar_mosaic"
        platform = "ALOS/ALOS-2"
        instrument = "PALSAR/PALSAR-2"
        cf = "83.0 dB"
        bandpaths = {
            "hh": hhpath,
            "hv": hvpath,
            "linci": lincipath,
            "mask": maskpath,
            "date": datepath,
        }
    else:
        product_name = "jers_sar_mosaic"
        platform = "JERS-1"
        instrument = "SAR"
        cf = "84.66 dB"
        bandpaths = {
            "hh": hhpath,
            "linci": lincipath,
            "mask": maskpath,
            "date": datepath,
        }

    properties = {
        "odc:product": product_name,
        "odc:region_code": region_code,
        "platform": platform,
        "instruments": [instrument],
        "cf": cf,
        "launchdate": launch_date,
        "start_datetime": f"{year}-01-01T00:00:00Z",
        "end_datetime": f"{year}-12-31-T23:59:59Z",
    }

    assets = {}
    for name, path in bandpaths.items():
        href = f"s3://{s3_destination}/{path}"
        assets[name] = pystac.Asset(
            href=href, media_type=pystac.MediaType.COG, roles=["data"]
        )

    item = create_stac_item(
        file_path,
        id=str(odc_uuid(shortname, "1", [], year=year, tile=file_key.split("_")[0])),
        properties=properties,
        assets=assets,
        with_proj=True,
    )
    item.set_self_href(stac_href)

    s3_dump(
        json.dumps(item.to_dict(), indent=2),
        item.self_href,
        ContentType="application/json",
        ACL="bucket-owner-full-control",
    )
    log.info(f"STAC written to {item.self_href}")


def upload_to_s3(s3_destination, files, log):
    log.info(f"Uploading to {s3_destination}")
    # Upload data
    for out_file in files:
        out_name = os.path.basename(out_file)
        dest = f"S3://{s3_destination}/{out_name}"
        log.info(f"Uploading file to {dest}")
        if "yaml" in out_name:
            content_type = "text/yaml"
        else:
            content_type = "image/tiff"
        s3_dump(
            data=open(out_file, "rb").read(),
            url=dest,
            ACL="bucket-owner-full-control",
            ContentType=content_type,
        )


def run_one(
    tile_string: str,
    base_dir: Path,
    s3_destination: str,
    update_metadata: bool,
    log: Logger,
):
    year = tile_string.split("/")[0]
    tile = tile_string.split("/")[1]

    workdir = base_dir / tile_string / "wrk"
    outdir = base_dir / tile_string / "out"

    s3_destination = f"{s3_destination}/{year}/{tile}"
    file_key = f"{tile}_{year[-2:]}"

    stac_self_href = f"s3://{s3_destination}/{file_key}.stac-item.json"

    if s3_head_object(stac_self_href) is not None and not update_metadata:
        log.info(f"{stac_self_href} already exists, skipping")
        return
    elif update_metadata:
        if int(year) > 2010:
            name = "{}_{}_sl_{}_F02DAR.tif".format(tile, year[-2:], "HH")
        else:
            name = "{}_{}_sl_{}.tif".format(tile, year[-2:], "HH")
        one_file = f"s3://{s3_destination}/{name}"

        if s3_head_object(one_file) is not None:
            # Data file exists, so we can update metadata
            log.info(f"{one_file} exists, updating metadata only")
            write_stac(s3_destination, one_file, file_key, year, log)
            # Finish here, we don't need to create the data files
            return
        else:
            # Nothing to see here, keep on walking!
            log.info(f"{one_file} does not exist, continuing with data creation.")

    try:
        log.info(f"Starting up process for tile {tile_string}")
        make_directories([workdir, outdir], log)
        download_files(workdir, year, tile, log)
        list_of_cogs = combine_cog(workdir, outdir, tile, year, log)
        upload_to_s3(s3_destination, list_of_cogs, log)
        write_stac(s3_destination, list_of_cogs[0], file_key, year, log)
        delete_directories([workdir, outdir], log)
    except Exception:
        log.exception(f"Job failed for tile {tile_string}")
        exit(1)


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
@click.option(
    "--update-metadata",
    "-u",
    is_flag=True,
    help="Only update metadata if the data already exists.",
)
def cli(tile_string, workdir, s3_bucket, update_metadata, s3_path):
    """
    Example command:

    download-alos-palsar --tile-string 2020/N10E010 -w /tmp/download -s example-bucket -p alos_palsar_mosaic
    """
    log = setup_logging()

    s3_destination = s3_bucket.rstrip("/").lstrip("s3://") + "/" + s3_path.rstrip("/")

    run_one(tile_string, Path(workdir), s3_destination, update_metadata, log)


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
