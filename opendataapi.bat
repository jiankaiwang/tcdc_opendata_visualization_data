@echo off

cd C:\Users\acer4755g\devops\opendataplatform
set errorLogPath=/home/jkw/opendataplatform/error.log

# open data platform api
python dengue.py >> %errorLogPath%
python diarrheapiechart.py >> %errorLogPath%
python enterovirus.py >> %errorLogPath%
python hivbc.py >> %errorLogPath%
python influlinechart.py >> %errorLogPath%

# line notify
python odapi_devops.py