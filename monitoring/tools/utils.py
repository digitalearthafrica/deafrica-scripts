import gzip
import logging

from odc.aws import s3_client, s3_ls_dir, s3_fetch


def find_latest_report(report_folder_path: str) -> str:
    """
    Function to find the latest gap report
    :return:(str) return the latest report file name
    """

    s3 = s3_client(region_name="af-south-1")

    report_files = list(s3_ls_dir(uri=report_folder_path, s3=s3))

    if not report_files:
        raise RuntimeError("Report not found!")

    report_files.sort()

    logging.info(f"Last report {report_files[-1]}")

    return report_files[-1]


def read_report(report_path: str, limit=None):
    """
    read the gap report
    """
    logging.info(f"limit - {limit}")

    if "update" in report_path:
        logging.info("FORCED UPDATE FLAGGED!")

    s3 = s3_client(region_name="af-south-1")
    missing_scene_file_gzip = s3_fetch(url=report_path, s3=s3)

    missing_scene_paths = [
        scene_path.strip()
        for scene_path in gzip.decompress(missing_scene_file_gzip)
        .decode("utf-8")
        .split("\n")
        if scene_path
    ]

    logging.info(f"Limited: {int(limit) if limit else 'No limit'}")

    if limit:
        missing_scene_paths = missing_scene_paths[: int(limit)]

    logging.info(f"Number of scenes found {len(missing_scene_paths)}")
    logging.info(f"Example scenes: {missing_scene_paths[0:10]}")

    return missing_scene_paths
