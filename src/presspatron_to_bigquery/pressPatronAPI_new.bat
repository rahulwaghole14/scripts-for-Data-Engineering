@echo off

REM Change to your project directory
cd "C:\projects\hexa-data-alteryx-workflows\src\presspatron_to_bigquery" || exit /b

REM Activate the Conda environment
call C:\ProgramData\miniconda3\Scripts\activate.bat python312

REM Run the 'load_data_to_bigquery_update.py' Python file
echo Running 'pressPatronAPI_new.py'
python "pressPatronAPI_new.py"

REM Check error level returned by the first Python script and raise to Alteryx
if %ERRORLEVEL% neq 0 (
    echo 'pressPatronAPI_new.py' script failed.
    exit /b 1
)

echo Done.
