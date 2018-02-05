# -*- coding: utf-8 -*-
#
# author : jiankaiwang
#

import py2mysql
import ConfigParser
import os
import datetime
import subprocess
import re
import sys

defaultConfigPath = "config/opendata.txt"
today = datetime.datetime.now().strftime("%Y-%m-%d")

if os.path.isfile(defaultConfigPath):
    config = ConfigParser.ConfigParser()
    config.read(defaultConfigPath)
    
def formatMsg():
    global today, querySuccessData, queryFailureData, exceptionData
    mainMsg = u'{}自動化開放資料API資料準備，共{}成功，共{}失敗，分別為{}，有{}尚在運行中，分別為{}。'
    
    failData = ""
    if len(queryFailureData['data']) > 0:
        failQueryName = []
        for i in range(0, len(queryFailureData['data']), 1):
            failQueryName.append(queryFailureData['data'][i]['servicename'])
        failData = ','.join(failQueryName)
    else:
        failData = u'(無)'
        
    exceptedData = ""
    if len(exceptionData['data']) > 0:
        failQueryName = []
        for i in range(0, len(exceptionData['data']), 1):
            failQueryName.append(exceptionData['data'][i]['servicename'])
        exceptedData = ','.join(failQueryName)
    else:
        exceptedData = u'(無)'
    
    return((mainMsg).format(\
        today,
        querySuccessData['data'][0]['allCount'], \
        len(queryFailureData['data']), \
        failData,
        len(exceptionData['data']), \
        exceptedData
        ))
        
py2my = py2mysql.py2mysql(\
                 config.get("mysql","server"), \
                 config.get("mysql","port"), \
                 config.get("mysql","user"), \
                 config.get("mysql","password"), \
                 config.get("mysql","db"))

if py2my.checkConnectionValid()['state'] == "success":
    # get success update
    querySuccessData = py2my.execsql(\
        "select count(*) as allCount from statusRecord where status = 'complete';", \
        (), True, True)
    # get failure update
    queryFailureData = py2my.execsql(\
        "select * from statusRecord where status = 'error';", \
        (), True, True)
    # get still operation (exception)
    exceptionData = py2my.execsql(\
        "select * from statusRecord where status = 'execution';", \
        (), True, True)
    # call Line notify to inform
    bashCommand = u'python LineNotify.py -c config\line.txt -n DevOps -m ' + formatMsg()
    bashCommand = bashCommand.encode('utf-8')
    try:     
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        searchObj = re.search(r'0', output.decode('utf-8'), re.M | re.I)
        #print(searchObj.group()[0])
        if len(searchObj.group()) > 0:
            sys.exit(0)
        else:
            sys.exit(2)
    except:
        sys.exit(99)
else:
    print("Can not connect to the od log server.")
    sys.exit(1)
    
    
    
    
    