# -*- coding: utf-8 -*-
"""
#
# author : jiankaiwang (https://jiankaiwang.no-ip.biz/)
# project : CKAN Visualization
# service : dengue
#
"""

import csv
import urllib
import urllib2
import py2mysql
import ntplib
import datetime
import general
import sys
import json

# service name (must be noticed if it is going to change)
serviceName = 'dengue'

# checker : check mysql connection statue
py2my = py2mysql.py2mysql(\
        general.mysqlServerInfo['host'], \
        general.mysqlServerInfo['port'], \
        general.mysqlServerInfo['user'], \
        general.mysqlServerInfo['pass'], \
        general.mysqlServerInfo['dbname']\
        )
if py2my.checkConnectionValid()['state'] != 'success':
    print(general.writeOutErrorLog(serviceName, py2my.checkConnectionValid()['info']))
    sys.exit()

# checker : starttimestamp
getDataDict = {'servicename' : serviceName, \
               'status' : general.normalizedNote['exec'], \
               'starttimestamp' : general.getCrtTimeStamp(), \
               'endtimestamp' : "", \
               'note' : general.normalizedNote['stmp']\
               }
getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['stmpflag'])
if getWriteState['state'] != 'success':
    print(general.writeOutErrorLog(serviceName, getWriteState['info']))

url = 'https://od.cdc.gov.tw/eic/Age_County_Gender_061.csv'
encode = 'utf8'

try:                 
    response = urllib2.urlopen(url)
    cr = csv.reader(response)
    
    # checker : fetch data
    getDataDict = {'servicename' : serviceName, \
                   'status' : general.normalizedNote['exec'], \
                   'endtimestamp' : "", \
                   'note' : general.normalizedNote['fdflagdesc']\
                   }
    getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
    if getWriteState['state'] != 'success':
        print(general.writeOutErrorLog(serviceName, getWriteState['info']))

except:
    # checker : fetch data
    getDataDict = {'servicename' : serviceName, \
                   'status' : general.normalizedNote['error'], \
                   'endtimestamp' : general.getCrtTimeStamp(), \
                   'note' : general.normalizedNote['fderror']\
                   }
    getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
    if getWriteState['state'] != 'success':
        print(general.writeOutErrorLog(serviceName, getWriteState['info']))

    # fetch data is failure
    sys.exit()


# 2016 : {1 : 0 , 2 : 0, ... 12 : 0}
ttl = {}

#
# desc : initial the dictionary for calculating number
#
crtYear = 0
crtMonth = 0

def getCrtDate():
    global crtYear, crtMonth
    
    response = 0
    
    try:
        # sync with ntp server
        c = ntplib.NTPClient()
        response = datetime.datetime.utcfromtimestamp(c.request('watch.stdtime.gov.tw').tx_time)
    except:
        response = datetime.datetime.now()
        
    crtYear = response.year
    crtMonth = response.month

getCrtDate()    
    
def initList(listobj, year):
    global crtYear, crtMonth
    
    if year not in listobj.keys():
        tmp = {}
        if year != crtYear:
            for i in range(1, 13, 1):
                tmp.setdefault(i, 0)
        else:
            for i in range(1, crtMonth + 1, 1):
                tmp.setdefault(i, 0)
        listobj.setdefault(year, tmp)
                

# start to parse each row on the data
# a[0].decode('big5')
header = []

for line in cr:
    
    if len(header) < 1:
        header = line
        continue
    
    year = (int)(line[1].decode(encode))
    month = (int)(line[2].decode(encode))
    dengueval = (int)(line[8].decode(encode))
    
    initList(ttl, year)
    
    ttl[year][month] += dengueval


# start to sum dengue patients
yearList = ttl.keys()

# checker : fetching data positive control flag
if len(yearList) < 1:
    # Fetching data is complete but data is not match
    getDataDict = {'servicename' : serviceName, \
                   'status' : general.normalizedNote['error'], \
                   'endtimestamp' : general.getCrtTimeStamp(), \
                   'note' : general.normalizedNote['fdpcerror']\
                   }
    getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
    if getWriteState['state'] != 'success':
        print(general.writeOutErrorLog(serviceName, getWriteState['info']))
        
    # the data is no more parsing
    sys.exit()
        
else:
    # Fetching data and checking data is complete
    getDataDict = {'servicename' : serviceName, \
                   'status' : general.normalizedNote['exec'], \
                   'endtimestamp' : "", \
                   'note' : general.normalizedNote['fdcheckcomp']\
                   }
    getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
    if getWriteState['state'] != 'success':
        print(general.writeOutErrorLog(serviceName, getWriteState['info']))



        
# checker : data preparation ready
getDataDict = {'servicename' : serviceName, \
               'status' : general.normalizedNote['exec'], \
               'endtimestamp' : "", \
               'note' : general.normalizedNote['dataready']\
               }
getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
if getWriteState['state'] != 'success':
    print(general.writeOutErrorLog(serviceName, getWriteState['info']))




# checker : data preparation ready
getDataDict = {'servicename' : serviceName, \
               'status' : general.normalizedNote['exec'], \
               'endtimestamp' : "", \
               'note' : general.normalizedNote['insertintodb']\
               }
getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
if getWriteState['state'] != 'success':
    print(general.writeOutErrorLog(serviceName, getWriteState['info']))




# start to insert data into the database
for yearIndex in range(0, len(yearList), 1):
    
    monthList = ttl[yearList[yearIndex]].keys()
    
    for monthIndex in range(0, len(monthList), 1):
       
        queryData = py2my.execsql(\
        "select * from dengue where year = %s and month = %s;", \
        (yearList[yearIndex], monthList[monthIndex]), True, True\
        )
        
        if queryData['state'] != 'success':
            print(general.writeOutErrorLog(serviceName, queryData['info']))
            
            # skip this data
            continue
        
        syncdb = 0
        if len(queryData['data']) > 0:
            # aleady existing
            syncdb = py2my.execsql(\
            "update dengue set dengueval = %s where year = %s and month = %s;", \
            (ttl[yearList[yearIndex]][monthList[monthIndex]], yearList[yearIndex], monthList[monthIndex]), False, False\
            ) 
        else:
            # insert a new entity
            syncdb = py2my.execsql(\
            "insert into dengue (year, month, dengueval) values (%s, %s, %s);", \
            (yearList[yearIndex], monthList[monthIndex], ttl[yearList[yearIndex]][monthList[monthIndex]]), False, False\
            )
        if syncdb['state'] != 'success':
            print(general.writeOutErrorLog(serviceName, syncdb['info']))

            
            
            
# checker : confirm api data - v1
try:                 
    apidata = urllib.urlopen(\
                            general.apiInfo['protocol'] \
                            + general.apiInfo['host'] \
                            + general.apiInfo['port'] \
                            + general.apiInfo['path'] \
                            + general.apiInfo['denguev1']
    )
    jsonData = json.loads(apidata.read())
    
    if len(jsonData) < 1:
        
        getDataDict = {'servicename' : serviceName, \
                       'status' : general.normalizedNote['error'], \
                       'endtimestamp' : general.getCrtTimeStamp(), \
                       'note' : general.normalizedNote['apicheckfail']\
                       }
        getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
        if getWriteState['state'] != 'success':
            print(general.writeOutErrorLog(serviceName, getWriteState['info']))
            
        # api check is failure
        sys.exit()
    
    else:
        getDataDict = {'servicename' : serviceName, \
                       'status' : general.normalizedNote['exec'], \
                       'endtimestamp' : "", \
                       'note' : general.normalizedNote['apicheckdesc']\
                       }
        getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
        if getWriteState['state'] != 'success':
            print(general.writeOutErrorLog(serviceName, getWriteState['info']))

except:

    getDataDict = {'servicename' : serviceName, \
                   'status' : general.normalizedNote['error'], \
                   'endtimestamp' : general.getCrtTimeStamp(), \
                   'note' : general.normalizedNote['apicheckfail']\
                   }
    getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
    if getWriteState['state'] != 'success':
        print(general.writeOutErrorLog(serviceName, getWriteState['info']))

    # fetch data is failure
    sys.exit()

    

# checker : confirm api data - v2
try:                 
    apidata = urllib.urlopen(\
                            general.apiInfo['protocol'] \
                            + general.apiInfo['host'] \
                            + general.apiInfo['port'] \
                            + general.apiInfo['path'] \
                            + general.apiInfo['denguev2']
    )
    jsonData = json.loads(apidata.read())
    
    if len(jsonData) < 1:
        
        getDataDict = {'servicename' : serviceName, \
                       'status' : general.normalizedNote['error'], \
                       'endtimestamp' : general.getCrtTimeStamp(), \
                       'note' : general.normalizedNote['apicheckfail'] + " (denguev2) "\
                       }
        getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
        if getWriteState['state'] != 'success':
            print(general.writeOutErrorLog(serviceName, getWriteState['info']))
            
        # api check is failure
        sys.exit()
    
    else:
        getDataDict = {'servicename' : serviceName, \
                       'status' : general.normalizedNote['exec'], \
                       'endtimestamp' : "", \
                       'note' : general.normalizedNote['apicheckdesc']\
                       }
        getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
        if getWriteState['state'] != 'success':
            print(general.writeOutErrorLog(serviceName, getWriteState['info']))

except:

    getDataDict = {'servicename' : serviceName, \
                   'status' : general.normalizedNote['error'], \
                   'endtimestamp' : general.getCrtTimeStamp(), \
                   'note' : general.normalizedNote['apicheckfail'] + " (denguev2) "\
                   }
    getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
    if getWriteState['state'] != 'success':
        print(general.writeOutErrorLog(serviceName, getWriteState['info']))

    # fetch data is failure
    sys.exit()



# checker : end time stamp
getDataDict = {'servicename' : serviceName, \
               'status' : general.normalizedNote['comp'], \
               'endtimestamp' : general.getCrtTimeStamp(), \
               'note' : general.normalizedNote['etmp']\
               }
getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['etmpflag'])
if getWriteState['state'] != 'success':
    print(general.writeOutErrorLog(serviceName, getWriteState['info']))


















