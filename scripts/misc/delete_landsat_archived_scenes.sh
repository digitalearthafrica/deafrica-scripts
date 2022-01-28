#!/bin/bash

set -e

<<'###'
Read landsat archived report and perform s3 cleanup

pre-req:
- read/delete access to `deafrica-landsat` and `deafrica-services` s3 buckets

steps:
1. Download and read latest archived report from s3
2. For each dataset, get the s3 location and delete scene
###

export AWS_DEFAULT_REGION="af-south-1"

ENV=${ENV:-"dev"}
REPORT_DIR=${PWD}/reports/${ENV}

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
  echo "----------------------------------------------"
  scene_location=$(echo $archived_location | sed 's![^/]*$!!')
  echo "delete s3 scene: $scene_location"
  if $dryrun; then
    aws s3 rm --dryrun $scene_location --recursive
  else
    aws s3 rm $scene_location --recursive
  fi
done
echo "----------------------------------------------"

