"""
Download the Copernicus Global Land Service Lake Water Quality COG or
NetCDF files, crop and convert to Cloud Optimized Geotiffs, and push to an S3 bucket.
"""

import json
import os
import sys
import warnings

import click
import pandas as pd
import rioxarray
import xarray as xr
from odc.geo.xr import assign_crs
from rasterio.errors import NotGeoreferencedWarning
from s3fs import S3FileSystem
from tqdm import tqdm

from deafrica.data.cgls_lwq.constants import (
    COG_MANIFEST_FILE_URLS,
    NETCDF_MANIFEST_FILE_URLS,
)
from deafrica.data.cgls_lwq.filename_parser import (
    get_output_cog_url_from_cog,
    get_output_cog_url_from_netcdf,
)
from deafrica.data.cgls_lwq.tiles import (
    get_africa_tiles,
)
from deafrica.io import (
    check_directory_exists,
    check_file_exists,
    get_basename,
    get_filesystem,
    is_local_path,
    join_url,
)
from deafrica.logs import setup_logging
from deafrica.utils import split_tasks

# Suppress the warning
warnings.filterwarnings("ignore", category=NotGeoreferencedWarning)


@click.command(
    "download-cgls-lwq-cogs",
    no_args_is_help=True,
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    show_default=True,
    help=(
        "If overwrite is True, tasks that have already been processed will be rerun."
    ),
)
@click.option(
    "--url-filter",
    default=None,
    show_default=True,
    type=str,
    help="Filter to select cog or netcdf urls to download cropped cogs for.",
)
@click.argument(
    "product-name",
    type=str,
)
@click.argument(
    "output-dir",
    type=str,
)
@click.argument(
    "max-parallel-steps",
    type=int,
)
@click.argument(
    "worker-idx",
    type=int,
)
def download_cogs(
    overwrite: bool,
    url_filter: str,
    product_name: str,
    output_dir: str,
    max_parallel_steps: int,
    worker_idx: int,
):
    """
    Download the Copernicus Global Land Service Lake Water Quality datasets
    corresponding to the ODC product PRODUCT_NAME, crop and convert to Cloud
    Optimized Geotiffs (COGs), and push to OUTPUT_DIR.

    MAX_PARALLEL_STEPS indicates the total number of parallel workers
    processing tasks, and WORKER_IDX indicates the index of this worker
    (0-indexed).
    """
    # Setup logging level
    log = setup_logging()

    if product_name not in NETCDF_MANIFEST_FILE_URLS.keys():
        error = (
            f"Manifest file url not configured for the product {product_name}",
            f"Expected one of the following: {', '.join(list(NETCDF_MANIFEST_FILE_URLS.keys()))}.",
        )
        log.error(error)
        raise NotImplementedError(error)

    # Read COG urls available for the product
    try:
        manifest_file_url = COG_MANIFEST_FILE_URLS[product_name]
    except KeyError:
        log.warning(
            f"COG manifest file not found for product {product_name}, switching to netcdf manifest file."
        )
        manifest_file_url = NETCDF_MANIFEST_FILE_URLS[product_name]

    manifest_file = pd.read_csv(manifest_file_url, sep=";")

    all_dataset_urls = manifest_file["s3_path"].to_list()
    log.info(f"Found {len(all_dataset_urls)} datasets in the manifest file")

    # Apply filter
    if url_filter:
        all_dataset_urls = [i for i in all_dataset_urls if url_filter in i]
        if len(all_dataset_urls) < 1:
            raise ValueError(
                f"No dataset urls found in manifest file that match the filter '{url_filter}'"
            )
        else:
            log.info(
                f"Found {len(all_dataset_urls)} dataset urls in the manifest file that match the filter '{url_filter}'"
            )

    dataset_urls = split_tasks(all_dataset_urls, max_parallel_steps, worker_idx)

    if not dataset_urls:
        log.warning(f"Worker {worker_idx} has no tasks to process. Exiting.")
        sys.exit(0)

    log.info(f"Worker {worker_idx} processing {len(dataset_urls)} tasks")

    # Define the tiles over Africa
    if "300m" in dataset_urls[0]:
        grid_res = 300
    elif "100m" in dataset_urls[0]:
        grid_res = 100

    tiles = get_africa_tiles(grid_res)

    # Configure access to the CDSE s3 buckets.
    s3_fs = S3FileSystem(
        key=os.environ["CDSE_AWS_ACCESS_KEY_ID"],
        secret=os.environ["CDSE_AWS_SECRET_ACCESS_KEY"],
        endpoint_url="https://eodata.dataspace.copernicus.eu",
    )

    failed_tasks = []
    for idx, dataset_url in enumerate(dataset_urls):
        log.info(f"Processing {dataset_url} {idx + 1}/{len(dataset_urls)}")

        if "_cog" in get_basename(dataset_url):
            # Get the bands making up the dataset
            cog_urls = [f"s3://{i}" for i in s3_fs.ls(dataset_url)]
            log.info(f"Found {len(cog_urls)} bands for the dataset {dataset_url}")

            for cog_url in cog_urls:
                band_name = cog_url.split("-")[-1].split("_")[0]
                log.info(f"Processing band {band_name} from {cog_url}")

                try:
                    # Open the file handle from s3fs
                    with s3_fs.open(cog_url, "rb") as f:
                        da = rioxarray.open_rasterio(f, chunks=True)
                        da = da.squeeze()

                        if "spatial_ref" in list(da.coords):
                            crs_coord_name = "spatial_ref"
                        else:
                            crs_coord_name = "crs"

                        crs = da.rio.crs

                        if crs is None:
                            # Assumption drawn from product manual is
                            # data is either in EPSG:4326 or OGC:CRS84
                            if list(da.dims)[0] in ["y", "lat", "latitude"]:
                                crs = "EPSG:4326"
                            elif list(da.dims)[0] in ["x", "lon", "longitude"]:
                                crs = "OGC:CRS84"

                        da = assign_crs(da, crs, crs_coord_name=crs_coord_name)

                        # Get attributes to be used in tiled COGs
                        attrs = da.attrs
                        exclude = [
                            "lon#",
                            "lat#",
                            "number_of_regions",
                            "TileSize",
                            "NETCDF_",
                            "coordinates",
                        ]
                        filtered_attrs = {
                            k: v
                            for k, v in attrs.items()
                            if not any(sub.lower() in k.lower() for sub in exclude)
                        }
                        da.attrs = filtered_attrs

                        with tqdm(
                            iterable=tiles,
                            desc=f"Cropping band {band_name} COG file",
                            total=len(tiles),
                        ) as tiles:
                            for tile in tiles:
                                tile_idx, tile_geobox = tile

                                output_cog_url = get_output_cog_url_from_cog(
                                    product_name, output_dir, cog_url, tile_idx
                                )
                                if not overwrite:
                                    if check_file_exists(output_cog_url):
                                        continue

                                cropped_da = da.odc.crop(
                                    tile_geobox.extent.to_crs(da.odc.geobox.crs)
                                ).compute()

                                # Write cog files
                                if is_local_path(output_cog_url):
                                    cropped_da.odc.write_cog(
                                        fname=output_cog_url,
                                        overwrite=True,
                                        tags=filtered_attrs,
                                    )
                                else:
                                    cog_bytes = cropped_da.odc.write_cog(
                                        fname=":mem:",
                                        overwrite=True,
                                        tags=filtered_attrs,
                                    )
                                    fs = get_filesystem(output_cog_url, anon=False)
                                    with fs.open(output_cog_url, "wb") as f:
                                        f.write(cog_bytes)

                        log.info(f"Written COGs for band {band_name}")
                except Exception as error:
                    log.exception(error)
                    error_msg = f"Failed to generate cogs for the source file {cog_url}"
                    log.error(error_msg)
                    failed_tasks.append(error_msg)

        if "_nc" in get_basename(dataset_url):
            netcdf_url = [f"s3://{i}" for i in s3_fs.ls(dataset_url)][0]

            log.info("Loading netcdf file (wait time 8-10 minutes)...")
            # Open the file handle from s3fs
            # This could take about 8min-10min
            with s3_fs.open(netcdf_url, "rb") as f:
                ds = xr.open_dataset(f, chunks="auto")
                ds = ds.squeeze()
                log.info("Done.")

                if "spatial_ref" in list(ds.coords):
                    crs_coord_name = "spatial_ref"
                else:
                    crs_coord_name = "crs"

                crs = ds.rio.crs

                if crs is None:
                    # Assumption drawn from product manual is
                    # data is either in EPSG:4326 or OGC:CRS84
                    if list(ds.dims)[0] in ["y", "lat", "latitude"]:
                        crs = "EPSG:4326"
                    elif list(ds.dims)[0] in ["x", "lon", "longitude"]:
                        crs = "OGC:CRS84"

                ds = assign_crs(ds, crs, crs_coord_name=crs_coord_name)
                dataset_attrs = ds.attrs

                band_names_filter = ["crs"]
                band_names = [
                    i for i in list(ds.data_vars) if i not in band_names_filter
                ]
                log.info(f"Found {len(band_names)} bands for the dataset {dataset_url}")

                for band_name in band_names:
                    log.info(f"Processing band {band_name}")

                    try:
                        da = ds[band_name]
                        # Get attributes to be used in tiled COGs
                        band_attrs = da.attrs
                        attrs = {**dataset_attrs, **band_attrs}
                        exclude = [
                            "lon#",
                            "lat#",
                            "number_of_regions",
                            "TileSize",
                            "NETCDF_",
                            "coordinates",
                        ]
                        filtered_attrs = {
                            k: v
                            for k, v in attrs.items()
                            if not any(sub.lower() in k.lower() for sub in exclude)
                        }
                        da.attrs = filtered_attrs

                        with tqdm(
                            iterable=tiles,
                            desc=f"Cropping band {band_name} NetCDF file",
                            total=len(tiles),
                        ) as tiles:
                            for tile in tiles:
                                tile_idx, tile_geobox = tile

                                output_cog_url = get_output_cog_url_from_netcdf(
                                    product_name,
                                    output_dir,
                                    netcdf_url,
                                    band_name,
                                    tile_idx,
                                )
                                if not overwrite:
                                    if check_file_exists(output_cog_url):
                                        continue

                                cropped_da = da.odc.crop(
                                    tile_geobox.extent.to_crs(da.odc.geobox.crs)
                                )

                                # Write cog files
                                if is_local_path(output_cog_url):
                                    cropped_da.odc.write_cog(
                                        fname=output_cog_url,
                                        overwrite=True,
                                        tags=filtered_attrs,
                                    )
                                else:
                                    cog_bytes = cropped_da.odc.write_cog(
                                        fname=":mem:",
                                        overwrite=True,
                                        tags=filtered_attrs,
                                    )
                                    fs = get_filesystem(output_cog_url, anon=False)
                                    with fs.open(output_cog_url, "wb") as f:
                                        f.write(cog_bytes)

                        log.info(f"Written COGs for band {band_name}")
                    except Exception as error:
                        log.exception(error)
                        error_msg = f"Failed to generate cogs for the source file {netcdf_url} band {band_name}"
                        log.error(error_msg)
                        failed_tasks.append(error_msg)
    if failed_tasks:
        failed_tasks_json_array = json.dumps(failed_tasks)

        tasks_directory = "/tmp/"
        failed_tasks_output_file = join_url(tasks_directory, "failed_tasks")

        fs = get_filesystem(path=tasks_directory, anon=False)

        if not check_directory_exists(path=tasks_directory):
            fs.mkdirs(path=tasks_directory, exist_ok=True)
            log.info(f"Created directory {tasks_directory}")

        with fs.open(failed_tasks_output_file, "a") as file:
            file.write(failed_tasks_json_array + "\n")
        log.info(f"Failed tasks written to {failed_tasks_output_file}")

        raise RuntimeError(f"{len(failed_tasks)} tasks failed")
    else:
        log.info(f"Worker {worker_idx} completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    download_cogs()
