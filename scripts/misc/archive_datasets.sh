#!/bin/bash

set -e

<<'###'
Archive datasets and delete scenes by providing a list of dataset s3 uri file

NOTE:
- Archiving of dataset is slow process. So if you have long list of datasets to archive
  then consider spliting input file (i.e. landsat_orphan_<date>.txt) into multiple files and
  execute script in parallel.
- Converting this script into python with multi thread is another option too.

pre-req:
- full access to s3 buckets for dataset search
- write access to s3 bucket for publishing archived dataset report
- admin access to odc database
- latest stable odc datacube lib (version `1.8.6` to minimum)

steps:
- Read delete scenes file
- For each scene path, archive datasets and delete scene:
    * Get dataset-ids using s3-uri:
      datacube dataset uri-search <s3-uri>
    * For each dataset-ids:
      * generate archived report: dataset-id, product and location
        datacube dataset info <dataset-id> | yq '.'
      * archive dataset
        datacube dataset archive <derived-id>
    * delete scene
- Publish archived report to s3 - s3://deafrica-landsat/status-report/archived/
###

echo "start archiving datasets"

if [[ -z ${DB_ADMIN_PASSWORD} || -z ${DB_HOSTNAME} || -z ${ARCHIVED_REPORT_BUCKET} ]]; then
  echo "Please provide following env variables: DB_HOSTNAME, DB_ADMIN_PASSWORD, ARCHIVED_REPORT_BUCKET"
  exit 1;
fi

export DB_USERNAME=${DB_ADMIN_USER:-"odc_admin"}
export DB_DATABASE=${DB_DATABASE:-"odc"}
export DB_PORT=${DB_PORT:-"5432"}
export DB_PASSWORD=$DB_ADMIN_PASSWORD
export DB_HOSTNAME=$DB_HOSTNAME

export AWS_DEFAULT_REGION="af-south-1"

PRODUCT=${PRODUCT}
REPORT_DIR=${PWD}/reports/${PRODUCT}
SOURCE_REPORT_S3_PATH=$SOURCE_REPORT_S3_PATH
ARCHIVED_REPORT_BUCKET=$ARCHIVED_REPORT_BUCKET

# Verify db connection
datacube system check

# 1. Read Report - provide SOURCE_REPORT_FILE or SOURCE_REPORT_S3_PATH
mkdir -p $REPORT_DIR
if [ -z ${SOURCE_REPORT_FILE} ]; then
  SOURCE_REPORT_FILE=$(basename $SOURCE_REPORT_S3_PATH)
  aws s3 cp ${SOURCE_REPORT_S3_PATH} ${REPORT_DIR}/${SOURCE_REPORT_FILE}
fi
scene_paths=$(cat ${REPORT_DIR}/${SOURCE_REPORT_FILE})

date=$(date '+%Y-%m-%d')
archived_report_file_path="${REPORT_DIR}/archived_${date}.csv"
header="dataset-id,product,location"
touch $archived_report_file_path
if grep -q "${header}" "${archived_report_file_path}" ; then
   echo 'the header exists'
else
   echo $header > $archived_report_file_path
fi

ARCHIVED_REPORT_S3_PATH="s3://${ARCHIVED_REPORT_BUCKET}/status-report/archived/"

dryrun=${DRYRUN:-true}
is_delete_scene=${IS_DELETE_SCENE:-true}

# 2. For each scene path, archive datasets and delete scene:
for scene_path in $scene_paths; do
  echo "----------------------------------------------"
  echo "start archiving: $scene_path"
  dataset_ids=$(datacube dataset uri-search ${scene_path} | awk '{print $2}' | cut -d "=" -f2)

  for dataset_id in $dataset_ids; do
    # get dataset-id
    echo "dataset_id: $dataset_id"
    dataset_info=$(datacube dataset info $dataset_id | yq '.')

    # archive dataset
    echo "archive dataset: $dataset_id"
    if $dryrun; then
      datacube dataset archive --dry-run $dataset_id
    else
      datacube dataset archive $dataset_id
    fi

    # add entry to archived report: id, product and location
    echo $dataset_info | jq -r '.id,.product,.locations[0]' | paste -d, - - - >> $archived_report_file_path
  done

  if $is_delete_scene; then
    scene_location=$(echo $scene_path | sed 's![^/]*$!!')
    echo "delete s3 scene: $scene_location"
    if $dryrun; then
      aws s3 rm --dryrun $scene_location --recursive
    else
      aws s3 rm $scene_location --recursive
    fi
  fi
done
echo "----------------------------------------------"

# 3. publish archived report to s3
if $dryrun; then
  aws s3 cp --dryrun $archived_report_file_path $ARCHIVED_REPORT_S3_PATH
else
  if [ "${ENV}" == "prod" ]; then
    aws s3 cp $archived_report_file_path $ARCHIVED_REPORT_S3_PATH --acl bucket-owner-full-control
  else
    aws s3 cp $archived_report_file_path $ARCHIVED_REPORT_S3_PATH
  fi
fi
