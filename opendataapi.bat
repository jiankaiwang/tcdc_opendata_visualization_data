@echo off

cd C:\devops\tcdc_opendata_visualization_data
set errorLogPath=C:\devops\tcdc_opendata_visualization_data\error.log

# open data platform api
python dengue.py >> %errorLogPath%
python diarrheapiechart.py >> %errorLogPath%
python enterovirus.py >> %errorLogPath%
python hivbc.py >> %errorLogPath%
python influlinechart.py >> %errorLogPath%

# line notify
python odapi_devops.py