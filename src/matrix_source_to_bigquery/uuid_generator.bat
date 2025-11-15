@echo off

REM Change to your project directory
cd "C:\projects\hexa-data-alteryx-workflows\src" || exit /b

REM Activate the Conda environment
call C:\ProgramData\miniconda3\Scripts\activate.bat python312

REM Run the 'marketing_id_generator.py' Python file
echo Running 'marketing_id_generator.py'
python -m matrix_source_to_bigquery.marketing_id_generator

REM Check error level returned by the first Python script and raise to Alteryx
if %ERRORLEVEL% neq 0 (
    echo 'marketing_id_generator.py' script failed.
    exit /b 1
)

echo Done.
