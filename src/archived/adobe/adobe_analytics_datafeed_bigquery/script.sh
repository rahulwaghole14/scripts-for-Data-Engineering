#!/bin/bash

# Assuming gcAccount, gcProject, dataSetName, datasetLocation, dataDir, maxError are defined previously...

# Set GCS path and local directory
LOCAL_DIR="/home/roshan_bhaskhar"
datasetLocation="australia-southeast1"
dataSetName="cdw_raw_adobe_analytics_masthead"
timestamp=$(date +%Y%m)
bucket_name="hexa_datafeed_masthead_continuous"
reportname="hexa_datafeed_mastheadfairfaxnz-hexaoverall-production"
GCS_PATH="gs://${bucket_name}/${reportname}"
hit_data="hit_data.tsv"
table_name_prefix="data"
maxError=100
HOSTNAME="adobe-datafeed-bq-loader-1"
ZONE="australia-southeast1-b"

# for replatform
dataset_replatform="cdw_raw_adobe_analytics_replatform"
bucket_name_replatform="hexa_datafeed_replatform_continuous"
reportname_replatform="hexa_datafeed_replatform_fairfaxnzhexa-new-replatform"  # Update this when you have the report name
GCS_PATH_REPLATFORM="gs://${bucket_name_replatform}/${reportname_replatform}"

# Create separate subdirectories for each dataset
LOCAL_DIR_ORIGINAL="${LOCAL_DIR}/original"
LOCAL_DIR_REPLATFORM="${LOCAL_DIR}/replatform"
mkdir -p $LOCAL_DIR_ORIGINAL $LOCAL_DIR_REPLATFORM

# Clear contents of the subdirectories to ensure they are clear before use
echo "Clearing contents of $LOCAL_DIR_ORIGINAL"
rm -rf ${LOCAL_DIR_ORIGINAL}/*
echo "Contents of $LOCAL_DIR_ORIGINAL after clearing:"
ls -alh ${LOCAL_DIR_ORIGINAL}

echo "Clearing contents of $LOCAL_DIR_REPLATFORM"
rm -rf ${LOCAL_DIR_REPLATFORM}/*
echo "Contents of $LOCAL_DIR_REPLATFORM after clearing:"
ls -alh ${LOCAL_DIR_REPLATFORM}

# Save the output of the gsutil command to an array for original dataset
mapfile -t my_array < <(gsutil ls gs://${bucket_name}/*${reportname}_${timestamp}*.tar.gz)

# Loop for processing original dataset files
for file in "${my_array[@]}"; do
    echo "file"
    echo "${file}"

    # Download files from GCS to local subdirectory for original dataset
    gsutil -m cp -r $file $LOCAL_DIR_ORIGINAL
    cd $LOCAL_DIR_ORIGINAL

    declare -a lookupTables=("browser" "browser_type" "color_depth" "connection_type" "country" "event" "javascript_version" "languages" "operating_systems" "plugins" "resolution" "search_engines")
    filename=$(basename "$file")
    echo "filename"
    echo ${filename}
    filename_text="${filename/.tar.gz/.txt}"
    loc_of_tarfile="gs://${bucket_name}/${filename}"

    substring=${filename: -22}
    substring=${substring:0:11}
    timestamp_fileext=${substring//-/}
    echo ${timestamp_fileext}
    loc_of_manifest_file="gs://${bucket_name}/${filename_text}"

    tar -xzvf ${filename}

    for tableName in "${lookupTables[@]}"; do
        echo "Importing ${tableName}"
        bq load --location=${datasetLocation} --replace --field_delimiter="\t" --source_format=CSV ${dataSetName}.${tableName} ./${tableName}.tsv id:STRING,name:STRING
    done

    echo "Importing referrer_type"
    bq load --location=${datasetLocation} --replace --field_delimiter="\t" --source_format=CSV ${dataSetName}.referrer_type ./referrer_type.tsv id:STRING,name:STRING,type:STRING

    # Prepare schema definition based on columns_headers
    cat column_headers.tsv | sed 's/[^a-zA-Z0-9\t]/_/g' | sed $'s/\t/:STRING,/g' | sed 's/$/:STRING/' > schema.txt

    bq load --location=${datasetLocation} --replace --quote="" --field_delimiter="\t" --allow_jagged_rows --ignore_unknown_values --max_bad_records=${maxError} --source_format=CSV ${dataSetName}.${table_name_prefix}_${timestamp_fileext} ./$hit_data $(cat schema.txt)

    status=$?

    if [ $status -eq 0 ]; then
        echo "BQ load was successful"
        dest_for_gzip_files="gs://${bucket_name}/zipped_files/${filename}"
        dest_for_manifest_files="gs://${bucket_name}/manifest_files/${filename_text}"
        echo "destination of zipped files"
        echo ${dest_for_gzip_files}

        gsutil -m mv ${loc_of_tarfile} ${dest_for_gzip_files}
        gsutil -m mv ${loc_of_manifest_file} ${dest_for_manifest_files}

        # Remove local files
        find . -type f \( -name "*.tsv" -o -name "*.tar.gz" -o -name "*.txt" \) -exec rm -f {} +

    else
        echo "BQ load failed"
        # Optionally, you can exit the script if the command fails
        # exit 1
    fi

    # Return to main directory
    cd $LOCAL_DIR
done

# Save the output of the gsutil command for replatform dataset to an array
mapfile -t my_array_replatform < <(gsutil ls gs://${bucket_name_replatform}/*${reportname_replatform}_${timestamp}*.tar.gz)

# Loop for processing replatform dataset files
for file_replatform in "${my_array_replatform[@]}"; do
    echo "Processing file for replatform: $file_replatform"

    # Download files from GCS to local subdirectory for replatform dataset
    gsutil -m cp -r $file_replatform $LOCAL_DIR_REPLATFORM
    cd $LOCAL_DIR_REPLATFORM

    declare -a lookupTablesReplatform=("browser" "browser_type" "color_depth" "connection_type" "country" "event" "javascript_version" "languages" "operating_systems" "plugins" "resolution" "search_engines")

    # Similar processing steps as for the original dataset
    filename_replatform=$(basename "$file_replatform")
    filename_text_replatform="${filename_replatform/.tar.gz/.txt}"
    loc_of_tarfile_replatform="gs://${bucket_name_replatform}/${filename_replatform}"
    loc_of_manifest_file_replatform="gs://${bucket_name_replatform}/${filename_text_replatform}"

    substring_re=${filename_replatform: -22}
    substring_re=${substring_re:0:11}
    timestamp_fileext_re=${substring_re//-/}
    echo "Processing file for replatform date: $timestamp_fileext_re"

    tar -xzvf ${filename_replatform}

    for tableName in "${lookupTablesReplatform[@]}"; do
        echo "Importing ${tableName} for replatform"
        bq load --location=${datasetLocation} --replace --field_delimiter="\t" --source_format=CSV ${dataset_replatform}.${tableName} ./${tableName}.tsv id:STRING,name:STRING
    done

    echo "Importing referrer_type for replatform"
    bq load --location=${datasetLocation} --replace --field_delimiter="\t" --source_format=CSV ${dataset_replatform}.referrer_type ./referrer_type.tsv id:STRING,name:STRING,type:STRING

    # Prepare schema definition for hit_data
    cat column_headers.tsv | sed 's/[^a-zA-Z0-9\t]/_/g' | sed $'s/\t/:STRING,/g' | sed 's/$/:STRING/' > schema_replatform.txt

    bq load --location=${datasetLocation} --replace --quote="" --field_delimiter="\t" --allow_jagged_rows --ignore_unknown_values --max_bad_records=${maxError} --source_format=CSV ${dataset_replatform}.${table_name_prefix}_${timestamp_fileext_re} ./$hit_data $(cat schema_replatform.txt)

    status=$?

    if [ $status -eq 0 ]; then
        echo "BQ load was successful for replatform"
        dest_for_gzip_files_replatform="gs://${bucket_name_replatform}/zipped_files/${filename_replatform}"
        dest_for_manifest_files_replatform="gs://${bucket_name_replatform}/manifest_files/${filename_text_replatform}"
        echo "Destination of zipped files for replatform"
        echo ${dest_for_gzip_files_replatform}

        gsutil -m mv ${loc_of_tarfile_replatform} ${dest_for_gzip_files_replatform}
        gsutil -m mv ${loc_of_manifest_file_replatform} ${dest_for_manifest_files_replatform}

        # Remove local files
        find . -type f \( -name "*.tsv" -o -name "*.tar.gz" -o -name "*.txt" \) -exec rm -f {} +
    else
        echo "BQ load failed for replatform"
        # Optionally, you can exit the script if the command fails
        # exit 1
    fi

    # Return to main directory
    cd $LOCAL_DIR
done

# Stop the VM
gcloud compute instances stop ${HOSTNAME} --zone=${ZONE}
