#!/bin/bash

set -e

<<'###'
Read landsat archived report and perform s3 cleanup

pre-req:
- read/delete access to `deafrica-landsat` and `deafrica-services` s3 buckets
- admin access to odc database
- latest stable odc datacube lib (version `1.8.6` to minimum)

steps:
1. Download and read latest archived report from s3
2. For each dataset, get the s3 location
3. Find datasets for given s3 location and check its status
4. Delete scene only if it has all archived datasets
###

if [[ -z ${DB_ADMIN_PASSWORD} || -z ${DB_HOSTNAME} ]]; then
  echo "Please provide following env variables: DB_HOSTNAME and DB_ADMIN_PASSWORD"
  exit 1;
fi

export DB_USERNAME=${DB_ADMIN_USER:-"odc_admin"}
export DB_DATABASE=${DB_DATABASE:-"odc"}
export DB_PORT=${DB_PORT:-"5432"}
export DB_PASSWORD=$DB_ADMIN_PASSWORD
export DB_HOSTNAME=$DB_HOSTNAME

export AWS_DEFAULT_REGION="af-south-1"

ENV=${ENV:-"dev"}
REPORT_DIR=${PWD}/reports/${ENV}

# Verify db connection
datacube system check

echo "start deleting Landsat archived scenes from s3"

# 1. Read archived report
if [ -z ${LATEST_ARCHIVED_REPORT} ]; then
  ARCHIVED_REPORT_S3_PATH="s3://deafrica-landsat/status-report/archived/"
  LATEST_ARCHIVED_REPORT=$(aws s3 ls $ARCHIVED_REPORT_S3_PATH | grep "landsat_archived_" | sort | tail -n 1 | awk '{print $4}')
  aws s3 cp ${ARCHIVED_REPORT_S3_PATH}${LATEST_ARCHIVED_REPORT} ${REPORT_DIR}/${LATEST_ARCHIVED_REPORT}
fi
archived_locations=$(cat ${REPORT_DIR}/${LATEST_ARCHIVED_REPORT} | cut -d',' -f3 | awk '{if (NR!=1) {print}}')

dryrun=${DRYRUN:-true}

# 2. delete scenes
for archived_location in $archived_locations; do

  # 3. Find datasets for given s3 location and check their status
  dataset_ids=$(datacube dataset uri-search ${archived_location} | awk '{print $2}' | cut -d "=" -f2)
  is_delete_scene=true
  for dataset_id in $dataset_ids; do
    dataset_status=$(datacube dataset info $dataset_id | yq '.' | jq -r '.status')
    if [ "$dataset_status" == "active" ]; then
      is_delete_scene=false
      echo "----------------------------------------------"
      echo "skip deleting s3 scene due to active dataset($dataset_id): $scene_location"
      break
    fi
  done

  # 4. Delete scene only it has all archived datasets
  if $is_delete_scene; then
    echo "----------------------------------------------"
    scene_location=$(echo $archived_location | sed 's![^/]*$!!')
    echo "delete s3 scene: $scene_location"
    if $dryrun; then
      aws s3 rm --dryrun $scene_location --recursive
    else
      aws s3 rm $scene_location --recursive
    fi
  fi
done
echo "----------------------------------------------"
