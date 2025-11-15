## Module Name: `main`
### Author: `roshan`

### Description
This module is designed to orchestrate various data operations including file matching, directory creation, file extraction, data loading, and file movement. It interacts primarily with Google Cloud Storage and BigQuery to perform tasks like matching files of the day, creating necessary directories in a specified bucket, extracting files to GCS, loading various lookup tables, loading hit data into BigQuery, and moving processed tar files to a specified folder.

### Operations
The module performs the following operations:
1. **File Matching**: Utilizes `matchfiles_of_the_day` to match files of the day based on a given timestamp and report name.
2. **Directory Creation**: Uses `create_directory` to create necessary directories in a specified GCS bucket.
3. **File Extraction**: Employs `extract_file_to_gcs_v2` to extract files to Google Cloud Storage.
4. **Data Loading**: Leverages `load_lookup` to load various lookup tables and `loading_hit_data_v2` to load hit data into BigQuery.
5. **File Movement**: Utilizes `move_tarfile_to_folder` to move processed tar files to a specified folder.

### Usage
This module is intended to be run as a standalone script:
```sh
pyenv activate alteryx
pip install -r requirements.txt
python main.py
```
Ensure that all required environment variables are set before running the script, either in the environment or in a `.env` file using Python’s `os` module and the `dotenv` package.

### Schedule on Alteryx
The main.py file will run every 3hrs staring from 3:00AM everyday.

### Dependencies
- **google-cloud-storage**: To interact with Google Cloud Storage services.
- **google-cloud-bigquery**: To interact with Google BigQuery services.
- **python-dotenv**: To load environment variables from a `.env` file.
- **retrying**: To add retry behavior to your functions/methods.
- **pandas**: To handle and manipulate your data.
- **tarfile**: To read and write tar archives.
- **io**: To perform the core input/output stream operations.
- **re**: To perform regular expression operations.
- **time**: To perform various time-related tasks.
- **os**: To interact with the operating system, for reading or writing to the file system, reading environment variables, etc.
- **logging**: To log messages that will help in debugging and understanding the flow and state of the application.

Ensure that all the dependencies are installed in your Python environment before running the module. You can install the dependencies using pip specific to this program:

```sh
pip install google-cloud-storage google-cloud-bigquery python-dotenv retrying pandas
```

### Notes
- Ensure that the environment variables are correctly set up, and the `.env` file is properly configured if used.
