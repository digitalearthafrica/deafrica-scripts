import logging
from datetime import datetime

import click
import datacube
import toolz

from deafrica.io import check_directory_exists, get_filesystem, get_parent_dir, join_url
from deafrica.logs import setup_logging


@click.command(
    "find-duplicate-scenes",
    no_args_is_help=True,
)
@click.argument(
    "product",
    type=str,
)
@click.argument(
    "output-dir",
    type=str,
)
@click.option(
    "--log",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="WARNING",
    show_default=True,
    help="control the log level, e.g., --log=error",
)
def cli(
    product: str,
    output_dir: str,
    log: str,
):
    """
    Find duplicate datasets in the specified ODC PRODUCT and write the
    file path for their shared metadata document (s3 uri) to a text file
    in the OUTPUT_DIR directory.
    Datasets are considered duplicates if they have the same STAC file path.

    """
    log_level = getattr(logging, log.upper())
    _log = setup_logging(log_level)

    dc = datacube.Datacube()

    _log.info(f"Searching for datasets in product: {product}")
    datasets = dc.find_datasets_lazy(product=product)
    grouped_by_s3_uri = toolz.groupby(lambda ds: ds.uri, datasets)

    _log.info(f"Searching for duplicate datasets in product: {product}")
    datasets_to_delete = []
    for s3_uri, duplicate_datasets in grouped_by_s3_uri.items():
        if len(duplicate_datasets) > 1:
            datasets_to_delete.append(s3_uri)

    _log.info(f"{len(datasets_to_delete)} {product} scenes with duplicates")

    if datasets_to_delete:
        output_file = join_url(
            output_dir,
            "status-report",
            f"{product}_duplicate_datasets_{datetime.now().strftime('%Y-%m-%d')}.txt",
        )
        fs = get_filesystem(output_file, anon=False)

        parent_dir = get_parent_dir(output_file)
        if not check_directory_exists(parent_dir):
            fs.makedirs(parent_dir, exist_ok=True)

        with fs.open(output_file, "w") as file:
            for s3_uri in datasets_to_delete:
                file.write(f"{s3_uri}\n")

        _log.info(f"{product} duplicate dataset URIs written to {output_file}")
    else:
        _log.info(f"No duplicate datasets for {product} found.")
