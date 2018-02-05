# -*- coding: utf-8 -*-
"""
#
# author : jiankaiwang (https://jiankaiwang.no-ip.biz/)
# project : CKAN Visualization
# service : hivbc
#
"""

import csv
import urllib
import urllib2
import py2mysql
import general
import sys
import json


# service name (must be noticed if it is going to change)
serviceName = 'hivbc'

# checker : check mysql connection statue
py2my = py2mysql.py2mysql(\
        general.mysqlServerInfo['host'], \
        general.mysqlServerInfo['port'], \
        general.mysqlServerInfo['user'], \
        general.mysqlServerInfo['pass'], \
        general.mysqlServerInfo['dbname']\
        )
if py2my.checkConnectionValid()['state'] != 'success':
    print general.writeOutErrorLog(serviceName, py2my.checkConnectionValid()['info'])
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
    print general.writeOutErrorLog(serviceName, getWriteState['info'])



url = 'https://od.cdc.gov.tw/eic/Age_County_Gender_044.csv'
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
        print general.writeOutErrorLog(serviceName, getWriteState['info'])

except:
    # checker : fetch data
    getDataDict = {'servicename' : serviceName, \
                   'status' : general.normalizedNote['error'], \
                   'endtimestamp' : general.getCrtTimeStamp(), \
                   'note' : general.normalizedNote['fderror']\
                   }
    getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
    if getWriteState['state'] != 'success':
        print general.writeOutErrorLog(serviceName, getWriteState['info'])

    # fetch data is failure
    sys.exit()



    
# 2016 : {'0-19' : 0, '20-29' : 0, '30-39' : 0, '40-49' : 0, '50-59' : 0, '60-69' : 0, '70+' : 0}
ttl = {}

#
# desc : initial the dictionary for calculating number
#
def initList(listobj, year):
    
    if year not in listobj.keys():
        tmp = {'0-19' : 0, '20-29' : 0, '30-39' : 0, '40-49' : 0, '50-59' : 0, '60-69' : 0, '70+' : 0}
        listobj.setdefault(year, tmp)

# start to parse each row on the data
# a[0].decode(encode)
header = []

for line in cr:
    
    if len(header) < 1:
        header = line
        continue
    
    year = (int)(line[1].decode(encode))
    age = (line[6].decode(encode))
    
    initList(ttl, year)
    
    # prevent empty (or null)
    if line[5].decode(encode) == u'非本國籍':
        continue

    hivBCAge = (int)((age.split("-")[0]).split('+')[0])
    
    if hivBCAge == 20 or hivBCAge == 25:
        ttl[year]['20-29'] += (int)(line[7].decode(encode))
    elif hivBCAge == 30 or hivBCAge == 35:
        ttl[year]['30-39'] += (int)(line[7].decode(encode))
    elif hivBCAge == 40 or hivBCAge == 45:
        ttl[year]['40-49'] += (int)(line[7].decode(encode))
    elif hivBCAge == 50 or hivBCAge == 55:
        ttl[year]['50-59'] += (int)(line[7].decode(encode))
    elif hivBCAge == 60 or hivBCAge == 65:
        ttl[year]['60-69'] += (int)(line[7].decode(encode))
    elif hivBCAge == 70:
        ttl[year]['70+'] += (int)(line[7].decode(encode))
    else:
        ttl[year]['0-19'] += (int)(line[7].decode(encode))


# start to sum hiv patients
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
        print general.writeOutErrorLog(serviceName, getWriteState['info'])
        
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
        print general.writeOutErrorLog(serviceName, getWriteState['info'])


        
# convert to percentage
for yearindex in range(0, len(yearList), 1):
    
    leftPercent = 100
    totalVal = 0
    ageList = ttl[yearList[yearindex]].keys()
    
    # sum all patients
    for ageindex in range(0, len(ageList), 1):
        totalVal += ttl[yearList[yearindex]][ageList[ageindex]]

    # convert to percentage
    for ageindex in range(0, len(ageList)-1, 1):       
        ttl[yearList[yearindex]][ageList[ageindex]] = \
            (int)(round(((float)(ttl[yearList[yearindex]][ageList[ageindex]]) / (float)(totalVal)) * 100, 0))
        leftPercent = leftPercent - ttl[yearList[yearindex]][ageList[ageindex]]

    # the final one
    ttl[yearList[yearindex]][ageList[len(ageList)-1]] = leftPercent




# checker : data preparation ready
getDataDict = {'servicename' : serviceName, \
               'status' : general.normalizedNote['exec'], \
               'endtimestamp' : "", \
               'note' : general.normalizedNote['dataready']\
               }
getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
if getWriteState['state'] != 'success':
    print general.writeOutErrorLog(serviceName, getWriteState['info'])  




# checker : data preparation ready
getDataDict = {'servicename' : serviceName, \
               'status' : general.normalizedNote['exec'], \
               'endtimestamp' : "", \
               'note' : general.normalizedNote['insertintodb']\
               }
getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
if getWriteState['state'] != 'success':
    print general.writeOutErrorLog(serviceName, getWriteState['info'])          



# start to insert data into the database
for yearindex in range(0, len(yearList), 1):
    
    
    
    for ageindex in range(0, len(ageList), 1):
       
        queryData = py2my.execsql(\
        "select * from hivbc where year = %s and age = %s;", \
        (yearList[yearindex], ageList[ageindex]), True, True\
        )
        
        if queryData['state'] != 'success':
            print general.writeOutErrorLog(serviceName, queryData['info'])  
            
            # skip this data
            continue
        
        syncdb = 0
        if len(queryData['data']) > 0:
            # aleady existing
            syncdb = py2my.execsql(\
            "update hivbc set hivval = %s where year = %s and age = %s;", \
            (ttl[yearList[yearindex]][ageList[ageindex]], yearList[yearindex], ageList[ageindex]), False, False\
            ) 
        else:
            # insert a new entity
            syncdb = py2my.execsql(\
            "insert into hivbc (year, age, hivval) values (%s, %s, %s);", \
            (yearList[yearindex], ageList[ageindex], ttl[yearList[yearindex]][ageList[ageindex]]), False, False\
            )
        if syncdb['state'] != 'success':
            print general.writeOutErrorLog(serviceName, syncdb['info']) 


# checker : confirm api data
try:                 
    apidata = urllib.urlopen(\
                            general.apiInfo['protocol'] \
                            + general.apiInfo['host'] \
                            + general.apiInfo['port'] \
                            + general.apiInfo['path'] \
                            + general.apiInfo['hivbc']
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
            print general.writeOutErrorLog(serviceName, getWriteState['info'])
            
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
            print general.writeOutErrorLog(serviceName, getWriteState['info'])

except:

    getDataDict = {'servicename' : serviceName, \
                   'status' : general.normalizedNote['error'], \
                   'endtimestamp' : general.getCrtTimeStamp(), \
                   'note' : general.normalizedNote['apicheckfail']\
                   }
    getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
    if getWriteState['state'] != 'success':
        print general.writeOutErrorLog(serviceName, getWriteState['info'])

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
    print general.writeOutErrorLog(serviceName, getWriteState['info'])














