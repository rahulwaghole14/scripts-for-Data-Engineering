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


# Save the output of the gsutil command to an array
mapfile -t my_array < <(gsutil ls gs://${bucket_name}/*${reportname}_${timestamp}*.tar.gz)
len=${#my_array[@]}

mkdir -p $LOCAL_DIR  # Only need to ensure this once, not inside the loop
cd $LOCAL_DIR

for file in "${my_array[@]}"; do
    echo "file"
    echo "${file}"


    # Download files from GCS to Cloud Shell
    gsutil -m cp -r $file $LOCAL_DIR

    declare -a lookupTables=("browser" "browser_type" "color_depth" "connection_type" "country" "event" "javascript_version" "languages" "operating_systems" "plugins" "resolution" "search_engines")
    len=${#lookupTables[@]}
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

    for ((i=1; i<=${len}; i++)); do
        tableName="${lookupTables[$i-1]}"
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

        find . -type f \( -name "*.tsv" -o -name "*.tar.gz" -o -name "*.txt" \) -exec rm -f {} +


    else
        echo "BQ load failed"
        # Optionally, you can exit the script if the command fails
        # exit 1
    fi

done


# gcloud compute instances stop ${HOSTNAME} --zone=${ZONE}
