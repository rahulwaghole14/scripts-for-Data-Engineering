#!/bin/bash

BUCKET="hexa_datafeed_masthead_continuous"
DATE_LIMIT="20231206"

# Function to move files to the correct year-month subfolder
move_to_date_subfolder() {
    local file=$1
    local subfolder=$2

    # Extract the filename and date
    filename=$(basename "$file")
    file_date=$(echo $filename | grep -oP "\d{8}")

    # Check if the file date is before the limit
    if [[ "$file_date" < "${DATE_LIMIT}" ]]; then

        destination="gs://${BUCKET}/${subfolder}/${filename}"
        # Move the file to the year-month subfolder
        gsutil mv "${file}" "${destination}"
    fi
}

# Move .tar.gz and .txt files to respective subfolders
echo "Moving .tar.gz files..."
for file in $(gsutil ls "gs://${BUCKET}/*.tar.gz"); do
    move_to_date_subfolder "$file" "zipped_files"
done

echo "Moving .txt files..."
for file in $(gsutil ls "gs://${BUCKET}/*.txt"); do
    move_to_date_subfolder "$file" "manifest_files"
done

echo "File moving completed."
