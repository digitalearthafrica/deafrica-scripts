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
def cli(
    product: str,
    output_dir: str,
):
    """
    Find duplicate datasets in the specified ODC PRODUCT and write the
    file path for their shared metadata document (s3 uri) to a text file
    in the OUTPUT_DIR directory.
    Datasets are considered duplicates if they have the same STAC file path.

    """
    log = setup_logging()

    dc = datacube.Datacube()

    datasets = dc.find_datasets(product=product)

    grouped_by_s3_uri = toolz.groupby(lambda ds: ds.uri, datasets)

    datasets_to_delete = []
    for s3_uri, datasets in grouped_by_s3_uri.items():
        if len(datasets) > 1:
            datasets_to_delete.append(s3_uri)

    log.info(f"{len(datasets_to_delete)} {product} scenes with duplicates")

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

    log.info(f"{product} duplicate dataset URIs written to {output_file}")
