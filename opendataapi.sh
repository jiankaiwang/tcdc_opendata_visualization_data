#!/bin/bash

execPath=/home/jkw/opendataplatform
errorLogPath=/home/jkw/opendataplatform/error.log
cd $execPath

# open data platform api
python dengue.py >> $errorLogPath
python diarrheapiechart.py >> $errorLogPath
python enterovirus.py >> $errorLogPath
python hivbc.py >> $errorLogPath
python influlinechart.py >> $errorLogPath

# line notify
python odapi_devops.py