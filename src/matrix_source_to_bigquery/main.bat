@echo off

REM Change to your project directory
cd "C:\projects\hexa-data-alteryx-workflows\src\matrix_source_to_bigquery" || exit /b

REM Activate the Conda environment
call C:\ProgramData\miniconda3\Scripts\activate.bat python312

REM Run the 'load_data_to_bigquery_update.py' Python file
echo Running 'load_data_to_bigquery_update.py'
python "load_data_to_bigquery_update.py"

REM Check error level returned by the first Python script and raise to Alteryx
if %ERRORLEVEL% neq 0 (
    echo 'load_data_to_bigquery_update.py' script failed.
    exit /b 1
)

REM Run the 'data_comparison_and_clean_up.py' Python file
echo Running 'data_comparison_and_clean_up.py'
python "data_comparison_and_clean_up.py"

REM Check error level returned by the second Python script and raise to Alteryx
if %ERRORLEVEL% neq 0 (
    echo 'data_comparison_and_clean_up.py' script failed.
    exit /b 1
)

echo Done.
