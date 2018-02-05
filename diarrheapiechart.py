# -*- coding: utf-8 -*-
"""
#
# author : jiankaiwang (https://jiankaiwang.no-ip.biz/)
# project : CKAN Visualization
# service : diarrheapiechart
#
"""

import csv
import urllib
import urllib2
import py2mysql
import json
import general
import sys


# service name (must be noticed if it is going to change)
serviceName = 'diarrheapiechart'

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

    



url = 'https://od.cdc.gov.tw/eic/NHI_Diarrhea.csv'
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


# 2016 : {'0-4' : 0, '5-14' : 0, '15-24' : 0, '25-64' : 0, '65+' : 0}
ttl = {}

#
# desc : initial the dictionary for calculating number
#
def initList(listobj, year):
    
    if year not in listobj.keys():
        tmp = {'0-4' : 0, '5-14' : 0, '15-24' : 0, '25-64' : 0, '65+' : 0}
        listobj.setdefault(year, tmp)

# start to parse each row on the data
# a[0].decode(encode)
header = []

for line in cr:
    
    if len(header) < 1:
        header = line
        continue
    
    year = (int)(line[0].decode(encode))
    age = (line[3].decode(encode))
    
    initList(ttl, year)
    
    # prevent empty (or null)
    if line[2].decode(encode) == u'住院' or len(line[2].decode(encode)) <= 1:
        continue

    ttl[year][age] += (int)(line[5].decode(encode))


# start to calculate influ ratio
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
    
    ageList = ttl[yearList[yearindex]].keys()
    
    for ageindex in range(0, len(ageList), 1):
       
        queryData = py2my.execsql(\
        "select * from diarrheapiechart where year = %s and age = %s;", \
        (yearList[yearindex], ageList[ageindex]), True, True\
        )
        
        if queryData['state'] != 'success':
            print queryData['info']
            
            # skip this data
            continue
        
        syncdb = 0
        if len(queryData['data']) > 0:
            # aleady existing
            syncdb = py2my.execsql(\
            "update diarrheapiechart set diaval = %s where year = %s and age = %s;", \
            (ttl[yearList[yearindex]][ageList[ageindex]], yearList[yearindex], ageList[ageindex]), False, False\
            ) 
        else:
            # insert a new entity
            syncdb = py2my.execsql(\
            "insert into diarrheapiechart (year, age, diaval) values (%s, %s, %s);", \
            (yearList[yearindex], ageList[ageindex], ttl[yearList[yearindex]][ageList[ageindex]]), False, False\
            )
        if syncdb['state'] != 'success':
            print syncdb['info']



# checker : confirm api data
try:                 
    apidata = urllib.urlopen(\
                            general.apiInfo['protocol'] \
                            + general.apiInfo['host'] \
                            + general.apiInfo['port'] \
                            + general.apiInfo['path'] \
                            + general.apiInfo['diarrheapc']
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














