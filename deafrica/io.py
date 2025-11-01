"""
Utilities for interacting with local, cloud (S3, GCS), and HTTP filesystems
"""

import logging
import os
import posixpath
import re
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse
from yarl import URL

import fsspec
import requests
import yaml
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from odc.aws import s3_url_parse
from s3fs.core import S3FileSystem
from tqdm import tqdm

log = logging.getLogger(__name__)


def is_s3_path(path: str) -> bool:
    """
    Checks if a given path is an s3 URI.
    """
    fs, _ = fsspec.core.url_to_fs(path)
    return isinstance(fs, S3FileSystem)


def is_gcsfs_path(path: str) -> bool:
    """
    Checks if a given path is a gsutil URI.
    """
    fs, _ = fsspec.core.url_to_fs(path)
    return isinstance(fs, GCSFileSystem)


def is_http_url(path: str) -> bool:
    """
    Checks if a given path is a http(s) URL.
    """
    fs, _ = fsspec.core.url_to_fs(path)
    return isinstance(fs, HTTPFileSystem)


def is_local_path(path: str) -> bool:
    """
    Checks if a given path is a local storage path.
    """
    fs, _ = fsspec.core.url_to_fs(path)
    return isinstance(fs, LocalFileSystem)


def join_url(base, *paths) -> str:
    """
    Join two or more pathname components, inserting '/' as needed.
    """
    if is_local_path(base):
        return os.path.join(base, *paths)
    else:
        # Ensure urls join correctly
        return posixpath.join(base, *paths)


def get_basename(path: str):
    """
    Returns the final component of a pathname
    """
    if is_local_path(path):
        return os.path.basename(path)
    else:
        return posixpath.basename(path)


def get_parent_dir(path: str):
    """
    Returns the logical parent of the path.
    """
    if is_local_path(path):
        return str(Path(path).resolve().parent)
    else:
        return str(URL(path).parent)


def get_filesystem(
    path: str,
    anon: bool = True,
) -> S3FileSystem | LocalFileSystem | GCSFileSystem:
    """
    Instantiate a file-system based on the input path type.
    """
    if is_s3_path(path=path):
        fs = S3FileSystem(
            anon=anon,
            # Use profile only on sandbox
            # profile="default",
            s3_additional_kwargs={"ACL": "bucket-owner-full-control"},
        )
    elif is_gcsfs_path(path=path):
        if anon:
            fs = GCSFileSystem(token="anon")
        else:
            fs = GCSFileSystem()
    elif is_http_url(path):
        fs = HTTPFileSystem()
    elif is_local_path(path=path):
        fs = LocalFileSystem()
    return fs


def check_file_exists(path: str) -> bool:
    """
    Checks if a given path exists and is a file.
    """
    fs = get_filesystem(path=path, anon=True)
    if fs.exists(path) and fs.isfile(path):
        return True
    else:
        return False


def check_directory_exists(path: str) -> bool:
    """
    Checks if a given path exists and is a directory.
    """
    fs = get_filesystem(path=path, anon=True)
    if fs.exists(path) and fs.isdir(path):
        return True
    else:
        return False


def check_file_extension(path: str, accepted_file_extensions: list[str]) -> bool:
    """Check if the file extension for a given path is among the list
    of allowed file extensions"""
    _, file_extension = os.path.splitext(path)
    if file_extension.lower() in accepted_file_extensions:
        return True
    else:
        return False


def find_files_by_extension(
    directory_path: str,
    accepted_file_extensions: list[str],
    file_name_pattern: str = ".*",
) -> list[str]:
    """
    Recursively find files matching extensions and optional filename pattern.

    Parameters
    ----------
    directory_path : str
         Path to search (local, S3, or GCS).
    accepted_file_extensions : list[str]
        List of extensions to include (e.g., ['.tif', '.csv']).
    file_name_pattern : str, optional
        Regex pattern to filter file names (default: match all).

    Returns
    -------
    list[str]
        List of matching file paths.
    """
    file_name_pattern = re.compile(file_name_pattern)

    fs = get_filesystem(path=directory_path, anon=True)

    matched_files = []

    for root, dirs, files in fs.walk(directory_path):
        for file_name in files:
            if check_file_extension(
                path=file_name,
                accepted_file_extensions=accepted_file_extensions,
            ):
                if re.search(file_name_pattern, file_name):
                    matched_files.append(os.path.join(root, file_name))
                else:
                    continue
            else:
                continue

    if is_s3_path(path=directory_path):
        matched_files = [f"s3://{file}" for file in matched_files]
    elif is_gcsfs_path(path=directory_path):
        matched_files = [f"gs://{file}" for file in matched_files]
    return matched_files


def find_geotiff_files(directory_path: str, file_name_pattern: str = ".*") -> list[str]:
    """
    Recursively find geotiff files matching an optional filename pattern.
    """
    geotiff_file_extensions = [".tif", ".tiff", ".gtiff"]
    geotiff_file_paths = find_files_by_extension(
        directory_path=directory_path,
        accepted_file_extensions=geotiff_file_extensions,
        file_name_pattern=file_name_pattern,
    )
    return geotiff_file_paths


def find_json_files(directory_path: str, file_name_pattern: str = ".*") -> list[str]:
    """
    Recursively find json files matching an optional filename pattern.
    """
    json_file_extensions = [".json"]
    json_file_paths = find_files_by_extension(
        directory_path=directory_path,
        accepted_file_extensions=json_file_extensions,
        file_name_pattern=file_name_pattern,
    )
    return json_file_paths


def find_csv_files(directory_path: str, file_name_pattern: str = ".*") -> list[str]:
    """
    Recursively find csv files matching an optional filename pattern.
    """
    csv_file_extensions = [".csv"]
    csv_file_paths = find_files_by_extension(
        directory_path=directory_path,
        accepted_file_extensions=csv_file_extensions,
        file_name_pattern=file_name_pattern,
    )
    return csv_file_paths


def download_file_from_url(url: str, output_file_path: str, chunks: int = 100) -> str:
    """Download a file from a URL

    Parameters
    ----------
    url : str
        URL to download file from.
    output_file_path : str
        File path to download to.
    chunks : int, optional
        Chunk size in MB, by default 100

    Returns
    -------
    str
        The file path the file has been downloaded to.
    """
    fs = get_filesystem(output_file_path, anon=False)

    # Create the parent directories if they do not exist
    parent_dir = fs._parent(output_file_path)
    if not check_directory_exists(parent_dir):
        fs.makedirs(parent_dir, exist_ok=True)

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with fs.open(output_file_path, "wb") as f:
            with tqdm(
                desc=output_file_path,
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for chunk in r.iter_content(chunk_size=chunks * 1024**2):
                    size = f.write(chunk)
                    bar.update(size)

    return output_file_path


def get_gdal_vsi_prefix(file_path) -> str:
    # Based on file extension
    _, file_extension = os.path.splitext(file_path)
    if file_extension in [".zip"]:
        vsi_prefix_1 = "vsizip"
    elif file_extension in [".gz"]:
        vsi_prefix_1 = "vsigzip"
    elif file_extension in [".tar", ".tgz"]:
        vsi_prefix_1 = "vsitar"
    elif file_extension in [".7z"]:
        vsi_prefix_1 = "vsi7z"
    elif file_extension in [".rar"]:
        vsi_prefix_1 = "vsirar"
    else:
        vsi_prefix_1 = ""

    if vsi_prefix_1:
        vsi_prefix_1_file_path = f"/{vsi_prefix_1}/{file_path}"
    else:
        vsi_prefix_1_file_path = file_path

    # Network based
    if is_local_path(file_path):
        return vsi_prefix_1_file_path
    elif is_http_url(file_path):
        return f"/vsicurl/{vsi_prefix_1_file_path}"
    elif is_s3_path(file_path):
        return f"/vsis3/{vsi_prefix_1_file_path}"
    elif is_gcsfs_path(file_path):
        return f"/vsigs/{vsi_prefix_1_file_path}"
    else:
        NotImplementedError()


def gsutil_uri_to_public_url(uri: str) -> str:
    """Convert gsutil URI to a public URL"""
    loc = urlparse(uri)
    if loc.scheme not in ("gs", "gcs"):
        raise ValueError(f"{uri} is not a gsutil URI")
    else:
        bucket = loc.hostname
        key = re.sub("^[/]", "", loc.path)
        public_url = join_url("https://storage.googleapis.com/", bucket, key)
        return public_url


def s3_uri_to_public_url(s3_uri, region="af-south-1"):
    """Convert S3 URI to a public HTTPS URL"""
    bucket, key = s3_url_parse(s3_uri)
    return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"


def get_last_modified(uri: str, aws_region="af-south-1"):
    """Returns the Last-Modified timestamp
    of a given URL or URI if available."""
    if is_gcsfs_path(uri):
        url = gsutil_uri_to_public_url(uri)
    elif is_s3_path(uri):
        url = s3_uri_to_public_url(uri, aws_region)
    else:
        url = uri

    assert is_http_url(url)

    response = requests.head(url, allow_redirects=True)
    last_modified = response.headers.get("Last-Modified")
    if last_modified:
        return parsedate_to_datetime(last_modified)
    else:
        return None


def download_product_yaml(url: str) -> str:
    """
    Download a product definition file from a raw github url.

    Parameters
    ----------
    url : str
        URL to the product definition file

    Returns
    -------
    str
        Local file path of the downloaded product definition file

    """
    try:
        # Create output directory
        tmp_products_dir = "/tmp/products"
        if not check_directory_exists(tmp_products_dir):
            fs = get_filesystem(tmp_products_dir, anon=False)
            fs.makedirs(tmp_products_dir, exist_ok=True)
            logging.info(f"Created the directory {tmp_products_dir}")

        output_path = os.path.join(tmp_products_dir, os.path.basename(url))

        # Load product definition from url
        response = requests.get(url)
        response.raise_for_status()
        content = yaml.safe_load(response.content.decode(response.encoding))

        # Write to file.
        yaml_string = yaml.dump(
            content,
            default_flow_style=False,  # Ensures block format
            sort_keys=False,  # Keeps the original order
            allow_unicode=True,  # Ensures special characters are correctly represented
        )
        # Ensure it starts with "---"
        yaml_string = f"---\n{yaml_string}"

        with open(output_path, "w") as file:
            file.write(yaml_string)
        logging.info(f"Product definition file written to {output_path}")
        return Path(output_path).resolve()
    except Exception as e:
        logging.error(e)
        raise e


def fix_assets_links(stac_file: dict) -> dict:
    """
    Fix assets' links to point from gsutil URI to
    public URL

    Parameters
    ----------
    stac_file : dict
        Stac item from converting a dataset doc to stac using
        `eodatasets3.stac.to_stac_item`

    Returns
    -------
    dict
        Updated stac_item
    """
    # Fix links in assets
    assets = stac_file["assets"]
    for measurement in assets.keys():
        measurement_url = assets[measurement]["href"]
        if is_gcsfs_path(measurement_url):
            new_measurement_url = measurement_url.replace(
                "gs://", "https://storage.googleapis.com/"
            )
            stac_file["assets"][measurement]["href"] = new_measurement_url

    return stac_file
