@echo off

REM Change to your project directory
cd "C:\projects\hexa-data-alteryx-workflows\src" || exit /b

REM Activate the Conda environment
call C:\ProgramData\miniconda3\Scripts\activate.bat python312

REM Run python file
echo Running python module piano__conversions_to_bigquery.main
python -m piano.piano__conversions_to_bigquery.main

REM Check error level returned by Python script and raise to alteryx
if %ERRORLEVEL% neq 0 (
    echo Python script failed.
    exit /b 1
)

echo Done.
