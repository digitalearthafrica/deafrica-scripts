#!/bin/bash

set -e

<<'###'
Read landsat archived report and perform s3 cleanup

pre-req:
- read/write/delete access to `deafrica-landsat` and `deafrica-services` s3 buckets

steps:
1. Read latest archived report
2. For each dataset, get the s3 location and delete scene
###

AWS_DEFAULT_REGION="af-south-1"

echo "start deleting Landsat archived scenes from s3"

# 1. Read archived report
if [ -z LATEST_ARCHIVED_REPORT ]; then
  ARCHIVED_REPORT_S3_PATH="s3://deafrica-landsat/status-report/archived/"
  LATEST_ARCHIVED_REPORT=$(aws s3 ls $ARCHIVED_REPORT_S3_PATH | grep "landsat_archived_" | sort | tail -n 1 | awk '{print $4}')
  aws s3 cp $ARCHIVED_REPORT_S3_PATH$LATEST_ARCHIVED_REPORT $PWD/$LATEST_ARCHIVED_REPORT
fi
archived_locations=$(cat $PWD/$LATEST_ARCHIVED_REPORT | cut -d',' -f3 | awk '{if (NR!=1) {print}}')

# 2. delete scenes
for archived_location in $archived_locations; do
  echo "----------------------------------------------"
  scene_location=$(echo $archived_location | sed 's![^/]*$!!')
  echo "delete s3 scene: $scene_location"
  aws s3 rm --dryrun $scene_location --recursive
done
echo "----------------------------------------------"

