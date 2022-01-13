#!/bin/bash

set -e

<<'###'
Read landsat orphan report for Landsat 5, Landsat 7 and Landsat 8 and perform datasets cleanup

steps:
- Read orphan scenes path from orphan-cleanup report
- For each orphan scene path:
    * Get SR and ST stac json file uri
    * Get dataset-id using uri:
      datacube dataset uri-search <.json>
    * Get dataset info for given dataset-id to collect derived locations
      datacube dataset info --show-derived <dataset-id> | yq .derived[].locations[0]
    * archive dataset and derive datasets
      datacube dataset archive --archive-derived --dry-run <dataset-id>
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
orphan_report_file={orphan_report_file:-"landsat_5_orphan_cleanup_report.txt"}
orphan_scene_paths=`cat $orphan_report_file`

# 2. For each orphan scene path
for orphan_scene_path in $orphan_scene_paths; do
  echo "$orphan_scene_path"

  # get SR and ST stac files
  stac_files=`aws s3 ls $orphan_scene_path | grep -Ei '\.json$' | grep -Eiv '\_MTL.json$' | awk '{print $4}'`

  # for each stac file
  for stac_file in $stac_files; do
    echo "stack: $stac_file"
    # get dataset-id
    dataset_id=`datacube dataset uri-search ${orphan_scene_path}${stac_file} | awk '{print $2}' | cut -d "=" -f2`
    echo "dataset_id: $dataset_id"

    # collect derived locations
    derived_locations=`datacube dataset info --show-derived $dataset_id | yq .derived[].locations[0]`
    echo "derived_locations: $derived_locations"

    # archive dataset and derive datasets
    datacube dataset archive --archive-derived --dry-run $dataset_id

    # s3 cleanup
    for derived_location in $derived_locations; do
      derived_path=`echo $derived_location | tr -d '"' | sed 's![^/]*$!!'`
      echo "derived_path: $derived_path"
      echo "remove derived scene here!"
      # aws s3 rm $derived_path --recursive --dry-run
    done

    echo "remove orphan scene here!"
    # aws s3 rm $orphan_scene_path --recursive --dry-run
  done
done
