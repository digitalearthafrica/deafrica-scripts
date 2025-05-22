import json

from odc.aws import s3_client, s3_fetch, s3_ls_dir


def find_latest_report(
    report_folder_path: str, contains: str = None, not_contains: str = None
) -> str:
    """
    Function to find the latest gap report
    :return:(str) return the latest report file name
    """

    s3 = s3_client(region_name="af-south-1")

    report_files = list(s3_ls_dir(uri=report_folder_path, s3=s3))

    if contains is not None:
        report_files = [report for report in report_files if contains in report]

    if not_contains is not None:
        report_files = [report for report in report_files if not_contains not in report]

    report_files.sort()

    if not report_files:
        raise RuntimeError("Report not found!")

    return report_files[-1]


def read_report_missing_scenes(report_path: str, limit=None):
    """
    read the gap report
    """

    s3 = s3_client(region_name="af-south-1")
    report_json = s3_fetch(url=report_path, s3=s3)
    report_dict = json.loads(report_json)

    if report_dict.get("missing", None) is None:
        raise Exception("Missing scenes not found")

    missing_scene_paths = [
        scene_path.strip() for scene_path in report_dict["missing"] if scene_path
    ]

    if limit:
        missing_scene_paths = missing_scene_paths[: int(limit)]

    return missing_scene_paths
