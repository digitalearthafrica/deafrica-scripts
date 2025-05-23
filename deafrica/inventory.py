import csv
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from gzip import GzipFile
from io import BytesIO
from types import SimpleNamespace

import pyarrow.parquet as pq
from odc.aws import s3_client, s3_fetch, s3_ls_dir


def find_latest_manifest(prefix, s3, **kw) -> str:
    """
    Find latest manifest
    """
    manifest_dirs = sorted(s3_ls_dir(prefix, s3=s3, **kw), reverse=True)

    for d in manifest_dirs:
        if d.endswith("/"):
            leaf = d.split("/")[-2]
            if leaf.endswith("Z"):
                return d + "manifest.json"


def retrieve_manifest_files(key: str, s3, schema, file_format, **kw):
    """
    Retrieve manifest file and return a namespace

    namespace(
        Bucket=<bucket_name>,
        Key=<key_path>,
        LastModifiedDate=<date>,
        Size=<size>
    )
    """
    if file_format == "CSV" and schema is not None:
        bb = s3_fetch(key, s3=s3, **kw)
        gz = GzipFile(fileobj=BytesIO(bb), mode="r")
        csv_rdr = csv.reader(line.decode("utf8") for line in gz)
        for rec in csv_rdr:
            yield SimpleNamespace(**dict(zip(schema, rec)))
    elif file_format == "PARQUET" and schema is None:
        bb = s3_fetch(key, s3=s3, **kw)
        table = pq.read_table(BytesIO(bb))
        df = table.to_pandas()
        assert (table.schema.names == df.columns).all()
        for row in df.itertuples(index=False):
            row_as_dict = dict(zip(row._fields, row))
            yield SimpleNamespace(**row_as_dict)


def test_key(
    key: str,
    prefix: str = "",
    suffix: str = "",
    contains: str = "",
    multiple_contains: tuple[str, str] = None,
):
    """
    Test if key is valid
    """
    contains = [contains]
    if multiple_contains is not None:
        contains = multiple_contains

    if key.startswith(prefix) and key.endswith(suffix):
        for c in contains:
            if c in key:
                return True

    return False


def list_inventory(
    manifest,
    s3=None,
    prefix: str = "",
    suffix: str = "",
    contains: str = "",
    multiple_contains: tuple[str, str] = None,
    n_threads: int = None,
    **kw,
):
    """
    Returns a generator of inventory records

    manifest -- s3:// url to manifest.json or a folder in which case latest one is chosen.

    :param manifest: (str)
    :param s3: (aws client)
    :param prefix: (str)
    :param prefixes: (List(str)) allow multiple prefixes to be searched
    :param suffix: (str)
    :param contains: (str)
    :param n_threads: (int) number of threads, if not sent does not use threads
    :return: SimpleNamespace
    """
    # pylint: disable=too-many-locals
    s3 = s3 or s3_client()

    if manifest.endswith("/"):
        manifest = find_latest_manifest(manifest, s3, **kw)

    info = s3_fetch(manifest, s3=s3, **kw)
    info = json.loads(info)

    must_have_keys = {"fileFormat", "fileSchema", "files", "destinationBucket"}
    missing_keys = must_have_keys - set(info)
    if missing_keys:
        raise ValueError("Manifest file haven't parsed correctly")

    file_format = info["fileFormat"].upper()
    accepted_file_formats = ["CSV", "PARQUET"]
    if file_format not in accepted_file_formats:
        raise ValueError(f"Data is not in {' or '.join(accepted_file_formats)} format")

    s3_prefix = "s3://" + info["destinationBucket"].split(":")[-1] + "/"
    data_urls = [s3_prefix + f["key"] for f in info["files"]]

    if file_format == "CSV":
        schema = tuple(info["fileSchema"].split(", "))
    elif file_format == "PARQUET":
        # Schema parsing is skipped here
        # as it can be extracted from the parquet file.
        schema = None

    if n_threads:
        with ThreadPoolExecutor(max_workers=1000) as executor:
            tasks = [
                executor.submit(retrieve_manifest_files, key, s3, schema, file_format)
                for key in data_urls
            ]

            for future in as_completed(tasks):
                for namespace in future.result():
                    try:
                        key = namespace.Key
                    except AttributeError:
                        key = namespace.key
                    if test_key(
                        key,
                        prefix=prefix,
                        suffix=suffix,
                        contains=contains,
                        multiple_contains=multiple_contains,
                    ):
                        yield namespace

    else:
        for u in data_urls:
            logging.info(f"Retrieve manifest files for {u}")
            for namespace in retrieve_manifest_files(u, s3, schema, file_format):
                try:
                    key = namespace.Key
                except AttributeError:
                    key = namespace.key
                if test_key(
                    key,
                    prefix=prefix,
                    suffix=suffix,
                    contains=contains,
                    multiple_contains=multiple_contains,
                ):
                    yield namespace
