import logging
import math
import traceback

import click

from monitoring.tools.utils import find_latest_report, read_report, split_list_equally

log = logging.getLogger()
console = logging.StreamHandler()
log.addHandler(console)


@click.argument("report_folder_path", type=str, nargs=1)
@click.argument("limit", type=str, nargs=1)
@click.argument("max_pods", type=int, nargs=1)
@click.command("get-missing-scenes")
def cli(report_folder_path: str, limit: str, max_pods: int = 1):
    """
    Function to retrieve the latest gap report and return missing scenes
    """
    try:

        latest_report = find_latest_report(report_folder_path=report_folder_path)
        files = read_report(report_path=latest_report, limit=limit)
        return split_list_equally(list_to_split=files, num_inter_lists=max_pods)

    except Exception as error:
        log.exception(error)
        traceback.print_exc()
        raise error
