@echo off

REM Change to your project directory
cd "C:\projects\hexa-data-alteryx-workflows" || exit /b

REM Activate the Conda environment
call C:\ProgramData\miniconda3\Scripts\activate.bat python312

REM Run python file
echo Running python file
python src\adobe_analytics__sponsored_content_segment_update\main.py

REM Check error level returned by Python script and raise to alteryx
if %ERRORLEVEL% neq 0 (
    echo Python script failed.
    exit /b 1
)

echo Done.
