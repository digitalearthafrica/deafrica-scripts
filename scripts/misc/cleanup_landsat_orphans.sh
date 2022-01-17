#!/bin/bash

set -e

<<'###'
Read landsat orphan report for Landsat 5, Landsat 7 and Landsat 8 and perform datasets cleanup

pre-req:
- read/write/delete access to `deafrica-landsat` and `deafrica-services` s3 buckets
- admin access to odc database

steps:
- Read orphan scenes path from orphan-cleanup report
- For each orphan scene path:
    * Get SR and ST stac json file uri
    * Get dataset-id using uri:
      datacube dataset uri-search <.json>
    * Get dataset info for given dataset-id to collect derived - ids and locations
      datacube dataset info --show-derived <dataset-id> | yq '.derived[]'
    * archive landsat orphan dataset and derive datasets (only wofs_ls, fc_ls but ignoring other derivatives)
      datacube dataset archive --dry-run <derived-id>
      datacube dataset archive --dry-run <dataset-id>
    * cleanup s3 - dataset and derive datasets locations
###

echo "start cleanup orphan datasets"

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
orphan_report_file=${orphan_report_file:-"landsat_orphan_cleanup_2022-01-14.txt"}
orphan_scene_paths=`cat $PWD/$orphan_report_file`

# 2. For each orphan scene path
for orphan_scene_path in $orphan_scene_paths; do
  echo "----------------------------------------------"
  echo "start archiving: $orphan_scene_path"

  # get SR and ST stac files
  stac_files=`aws s3 ls $orphan_scene_path | grep -Ei '\.json$' | grep -Eiv '\_MTL.json$' | awk '{print $4}'`

  # for each stac file
  for stac_file in $stac_files; do
    echo "stac_file: $stac_file"

    # get dataset-id
    dataset_id=`datacube dataset uri-search ${orphan_scene_path}${stac_file} | awk '{print $2}' | cut -d "=" -f2`
    echo "dataset_id: $dataset_id"

    # derived clenaup
    # Note: only archive wofs and fc derived
    derived=`datacube dataset info --show-derived $dataset_id | yq '.derived[]'`
    if [ -n "$derived" ]; then
      # collect derived: ids and locations
      derived_items=`echo $derived | jq '. | select(.product=="wofs_ls" or .product=="fc_ls")'`
      derived_ids=`echo $derived_items | jq --raw-output '.id'`
      echo "derived_ids: $derived_ids"
      derived_locations=`echo $derived_items | jq --raw-output '.locations[0]'`
      echo "derived_locations: $derived_locations"

      # archive derived
      for derived_id in $derived_ids; do
        echo "archive derived dataset: $derived_id"
        datacube dataset archive --dry-run $derived_id
      done

      # s3 derived cleanup
      for derived_location in $derived_locations; do
        derived_path=`echo $derived_location | tr -d '"' | sed 's![^/]*$!!'`
        echo "cleanup derived s3 scene: $derived_path"
        aws s3 rm --dryrun $derived_path --recursive
      done
    fi

    # archive orphan
    echo "archive orphan dataset: $dataset_id"
    datacube dataset archive --dry-run $dataset_id
  done

  echo "cleanup orphan s3 scene: $orphan_scene_path"
  aws s3 rm --dryrun $orphan_scene_path --recursive
  echo "complete archiving: $orphan_scene_path"
done
echo "----------------------------------------------"
