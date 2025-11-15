@echo off

REM Change to your dbt project directory
cd "C:\projects\hexa-data-alteryx-workflows\src_docker\dbtcdwarehouse\scripts\alteryx" || exit /b

REM Activate the Conda environment. Replace 'myenv' with your environment's name.
call C:\ProgramData\miniconda3\Scripts\activate.bat cdwarehouse

REM Run python file
echo Running python file
python main_ad_revenue.py

REM Check error level returned by Python script
if %ERRORLEVEL% neq 0 (
    echo Python script failed.
    exit /b 1
)

echo Done.
