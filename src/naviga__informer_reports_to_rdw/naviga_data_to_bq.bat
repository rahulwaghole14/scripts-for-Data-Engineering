@echo off

REM Change to your project directory
cd "C:\projects\hexa-data-alteryx-workflows" || exit /b

REM Activate the Conda environment
call C:\ProgramData\miniconda3\Scripts\activate.bat python312

REM Check if the Conda environment was activated successfully
if %ERRORLEVEL% neq 0 (
    echo Failed to activate Conda environment.
    exit /b 1
)

REM Run python file
echo Running python naviga_data_to_bq
python -m src.naviga__informer_reports_to_rdw.naviga_data_to_bq

REM Check error level returned by Python script and raise to alteryx
if %ERRORLEVEL% neq 0 (
    echo Python script failed.
    exit /b 1
)

echo Done.
