#!/bin/bash

set -e

<<'###'
Read landsat orphan report for Landsat 5, Landsat 7 and Landsat 8 and archive datasets

pre-req:
- read/write/delete access to `deafrica-landsat` and `deafrica-services` s3 buckets
- admin access to odc database

steps:
- Read latest orphan report
- For each orphan path, start archiving datasets. Make sure to archive wofs and fc derived datasets and generate report:
    * Get dataset-ids using s3-uri:
      datacube dataset uri-search <s3-uri>
    * For each dataset-ids:
      * Get dataset info for given dataset-id
        datacube dataset info --show-derived <dataset-id> | yq '.'
      * Generate archived report for orphan and derived datasets: dataset-id, product and location
      * archive orphan dataset and derived datasets
        datacube dataset archive --dry-run <derived-id>
        datacube dataset archive --dry-run <dataset-id>
- Publish archived report to s3 - s3://deafrica-landsat/status-report/archived/
###

echo "start archiving Landsat orphan datasets"

if [[ -z ${DB_ADMIN_PASSWORD} || -z ${DB_HOSTNAME} ]]; then
  echo "Please provide following env variables: DB_HOSTNAME and DB_ADMIN_PASSWORD"
  exit 1;
fi

export DB_USERNAME=${DB_ADMIN_USER:-"odc_admin"}
export DB_DATABASE=${DB_DATABASE:-"odc"}
export DB_PORT=${DB_PORT:-"5432"}
export DB_PASSWORD=$DB_ADMIN_PASSWORD
export DB_HOSTNAME=$DB_HOSTNAME

# Verify db connection
datacube system check

# 1. Read Report
if [ -z LATEST_ORPHAN_REPORT ]; then
  ORPHAN_REPORT_PATH="s3://deafrica-landsat/status-report/orphaned/"
  LATEST_ORPHAN_REPORT=`aws s3 ls $ORPHAN_REPORT_PATH | grep "landsat_orphan_" | sort | tail -n 1 | awk '{print $4}'`
  aws s3 cp $ORPHAN_REPORT_PATH$LATEST_ORPHAN_REPORT $PWD/$LATEST_ORPHAN_REPORT
fi
orphan_scene_paths=`cat $PWD/$LATEST_ORPHAN_REPORT`

date=$(date '+%Y-%m-%d')
archived_report_file_path="$PWD/landsat_archive_${date}.csv"
echo "dataset-id,product,location" > $archived_report_file_path

ARCHIVED_REPORT_S3_PATH="s3://deafrica-landsat/status-report/archived/"

# 2. For each orphan scene path, archive orphan and derived datasets:
for orphan_scene_path in $orphan_scene_paths; do
  echo "----------------------------------------------"
  echo "start archiving: $orphan_scene_path"
  dataset_ids=`datacube dataset uri-search ${orphan_scene_path} | awk '{print $2}' | cut -d "=" -f2`

  for dataset_id in $dataset_ids; do
    # get dataset-id
    echo "dataset_id: $dataset_id"
    dataset_info=`datacube dataset info --show-derived $dataset_id | yq '.'`

    # add orphan dataset info to archive report: id, product and location
    echo $dataset_info | jq -r '.id,.product,.locations[0]' | tr '\n' ',' | sed -e 's|,$||' >> $archived_report_file_path

    # archive derived
    # Note: only archiving landsat wofs and fc derived
    derived=`echo $dataset_info | yq '.derived[]'`
    if [ -n "$derived" ]; then
      derived_datasets=`echo $derived | jq '. | select(.product=="wofs_ls" or .product=="fc_ls")'`

      # add derived datasets info to archive report: id, product and location
      echo $derived_datasets | jq -r '.id,.product,.locations[0]' | tr '\n' ',' | sed -e 's|,$||' >> $archived_report_file_path

      # archive derived
      derived_ids=`echo $derived_datasets | jq -r '.id'`
      for derived_id in $derived_ids; do
        echo "archive derived dataset: $derived_id"
        datacube dataset archive --dry-run $derived_id
      done
    fi

    # archive orphan
    echo "archive orphan dataset: $dataset_id"
    datacube dataset archive --dry-run $dataset_id
  done
done
echo "----------------------------------------------"

# 3. publish archived report to s3
aws s3 cp --dryrun $archived_report_file_path $ARCHIVED_REPORT_S3_PATH
