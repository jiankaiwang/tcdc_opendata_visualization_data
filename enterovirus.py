# -*- coding: utf-8 -*-
"""
#
# author : jiankaiwang (https://jiankaiwang.no-ip.biz/)
# project : CKAN Visualization
# service : enterovirus
#
"""

from BeautifulSoup import BeautifulSoup as Soup
from soupselect import select
import urllib
import re
import py2mysql
import general
import json
import sys

# service name (must be noticed if it is going to change)
serviceName = 'enterovirus'

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

yearWeekData = {\
                "yw" : [], \
                "CA" : [], \
                "CB" : [], \
                "ECHO" : [], \
                "EV68" : [], \
                "EV71" : [], \
                "NPEV" : [], \
                "Rhino" : [], \
                "Polio" : [], \
                "Positive" : [], \
                }

influLinChart = {"YearWeek" : [], \
                 "Coxsackie" : [], \
                 "Enterovirus" : [], \
                 "Others" : [], \
                 "Positive" : [] \
                 }
                
soup = ""
res = ""

try:                 
    soup = Soup(urllib.urlopen('https://nidss.cdc.gov.tw/ch/Default.aspx?op=1'))
    res = select(soup, 'li#chart4 script')
    
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

scriptRes = res[0].text
scriptRes = re.sub(r'\r', '', scriptRes, flags=re.IGNORECASE)
scriptRes = re.sub(r'\n', '', scriptRes, flags=re.IGNORECASE)
scriptRes = re.sub(r'\t', '', scriptRes, flags=re.IGNORECASE)
scriptRes = re.sub(r' ', '', scriptRes, flags=re.IGNORECASE)

resmatch = re.match(r"^.+categories:\s*\[(.*)\],\s*tickInterval.+$", scriptRes)
#print resmatch, resmatch.group(1)
yearWeekData["yw"] = resmatch.group(1).split(",")

#seriesmatch = re.match(r"^.+series:\s*\[(.*)\],\s*plotOptions.+$", scriptRes)
#print seriesmatch, seriesmatch.group(1)

caData = re.match(r"^.+\{name:'CA'\S*,data:\[(.*)\],tooltip:\{valueSuffix:'isolates'\}\},\{name:'CB'.+$", scriptRes)
#print caData, "CA", caData.group(1)
yearWeekData["CA"] = caData.group(1).split(",")

cbData = re.match(r"^.+\{name:'CB'\S*,data:\[(.*)\],tooltip:\{valueSuffix:'isolates'\}\},\{name:'ECHO'.+$", scriptRes)
#print cbData, "CB", cbData.group(1)
yearWeekData["CB"] = cbData.group(1).split(",")

echoData = re.match(r"^.+\{name:'ECHO'\S*,data:\[(.*)\],tooltip:\{valueSuffix:'isolates'\}\},\{name:'EV-D68'.+$", scriptRes)
#print echoData, "ECHO", echoData.group(1)
yearWeekData["ECHO"] = echoData.group(1).split(",")

ev68Data = re.match(r"^.+\{name:'EV-D68'\S*,data:\[(.*)\],tooltip:\{valueSuffix:'isolates'\}\},\{name:'EV71'.+$", scriptRes)
#print ev68Data, "EV68", ev68Data.group(1)
yearWeekData["EV68"] = ev68Data.group(1).split(",")

ev71Data = re.match(r"^.+\{name:'EV71'\S*,data:\[(.*)\],tooltip:\{valueSuffix:'isolates'\}\},\{name:'NPEV'.+$", scriptRes)
#print ev71Data, "EV71", ev71Data.group(1)
yearWeekData["EV71"] = ev71Data.group(1).split(",")

npevData = re.match(r"^.+\{name:'NPEV'\S*,data:\[(.*)\],tooltip:\{valueSuffix:'isolates'\}\},\{name:'Rhino'.+$", scriptRes)
#print npevData, "NPEV", npevData.group(1)
yearWeekData["NPEV"] = npevData.group(1).split(",")

rhinoData = re.match(r"^.+\{name:'Rhino'\S*,data:\[(.*)\],tooltip:\{valueSuffix:'isolates'\}\},\{name:'Polio'.+$", scriptRes)
#print rhinoData, "Rhino", rhinoData.group(1)
yearWeekData["Rhino"] = rhinoData.group(1).split(",")

polioData = re.match(r"^.+\{name:'Polio'\S*,data:\[(.*)\],tooltip:\{valueSuffix:'isolates'\}\},\{name:'%Positive'.+$", scriptRes)
#print polioData, "Polio", polioData.group(1)
yearWeekData["Polio"] = polioData.group(1).split(",")

posData = re.match(r"^.+\{name:'%Positive'\S*,data:\[(.*)\],tooltip:\{valueSuffix:'\%'\}\}.+$", scriptRes)
#print posData, "%Positive", posData.group(1)
yearWeekData["Positive"] = posData.group(1).split(",")

# checker : fetching data positive control flag
try:                 
    virusType = yearWeekData.keys()
    failCheck = False
    checkWeek = 0
    
    # positive checker : data entity is existing and counting data entities in each files are the same
    for key in virusType:
        if checkWeek == 0:
            checkWeek = len(yearWeekData[key])
        if len(yearWeekData[key]) < 1:
            # if there is no data
            failCheck = True
        elif checkWeek != len(yearWeekData[key]):
            # if data counting is not the same
            failCheck = True
            
    if failCheck:
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

except:
    # Fetching data is complete but data meets a unexcepted error.
    getDataDict = {'servicename' : serviceName, \
                   'status' : general.normalizedNote['error'], \
                   'endtimestamp' : general.getCrtTimeStamp(), \
                   'note' : general.normalizedNote['fdpcunexceperror']\
                   }
    getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
    if getWriteState['state'] != 'success':
        print general.writeOutErrorLog(serviceName, getWriteState['info'])

    # fetch data is failure
    sys.exit()

def retVal(num, typeofvalue):
    if num == "null":
        return -1
    if typeofvalue == "int":
        return (int)(num)
    elif typeofvalue == "float":
        return (float)(num)

for ywindex in range(0, len(yearWeekData["yw"]), 1):
    yearweek = re.sub(r"'", "", yearWeekData["yw"][ywindex], flags=re.IGNORECASE)
    
    influLinChart["YearWeek"].append( \
        yearweek[0:4] + '_' + yearweek[4:6] \
    )
    influLinChart["Coxsackie"].append(\
        retVal(yearWeekData["CA"][ywindex], "int") \
        + retVal(yearWeekData["CB"][ywindex], "int") \
    )
    influLinChart["Enterovirus"].append(\
        retVal(yearWeekData["EV68"][ywindex], "int") \
        + retVal(yearWeekData["EV71"][ywindex], "int") \
    )
    influLinChart["Others"].append(\
        retVal(yearWeekData["ECHO"][ywindex], "int") \
        + retVal(yearWeekData["NPEV"][ywindex], "int") \
        + retVal(yearWeekData["Rhino"][ywindex], "int") \
        + retVal(yearWeekData["Polio"][ywindex], "int") \
    )
    influLinChart["Positive"].append(\
        retVal(yearWeekData["Positive"][ywindex], "float")\
    )

# checker : data preparation ready
getDataDict = {'servicename' : serviceName, \
               'status' : general.normalizedNote['exec'], \
               'endtimestamp' : "", \
               'note' : general.normalizedNote['dataready']\
               }
getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
if getWriteState['state'] != 'success':
    print general.writeOutErrorLog(serviceName, getWriteState['info'])  
    
# database
maxYear = str(influLinChart['YearWeek'][len(influLinChart['YearWeek'])-1][0:4])

# checker : data preparation ready
getDataDict = {'servicename' : serviceName, \
               'status' : general.normalizedNote['exec'], \
               'endtimestamp' : "", \
               'note' : general.normalizedNote['insertintodb']\
               }
getWriteState = general.writeStatusIntoDB(py2my, getDataDict, general.normalizedNote['fdflag'])
if getWriteState['state'] != 'success':
    print general.writeOutErrorLog(serviceName, getWriteState['info'])  

# start to insert/update data    
for ywindex in range(0, len(influLinChart["YearWeek"]), 1):
    queryData = py2my.execsql("select * from enterovirus where yearweek = %s;", \
                              (influLinChart["YearWeek"][ywindex],), True, True)
    if queryData['state'] != "success":
        print general.writeOutErrorLog(serviceName, queryData['info'])

        # skip this entity
        continue

    syncdb = 0
    if len(queryData['data']) > 0:
        # already existing
        syncdb = py2my.execsql(\
        "update enterovirus set coxsackie = %s, enterovirus = %s, positive = %s, others = %s where yearweek = %s;", \
        (influLinChart["Coxsackie"][ywindex], \
         influLinChart["Enterovirus"][ywindex], \
         influLinChart["Positive"][ywindex], \
         influLinChart["Others"][ywindex], \
         influLinChart["YearWeek"][ywindex]), \
        False, False) 
    else:
        syncdb = py2my.execsql(\
        "insert into enterovirus (yearweek, coxsackie, enterovirus, positive, others) values (%s, %s, %s, %s, %s);", \
        (influLinChart["YearWeek"][ywindex], \
         influLinChart["Coxsackie"][ywindex], \
         influLinChart["Enterovirus"][ywindex], \
         influLinChart["Positive"][ywindex], 
         influLinChart["Others"][ywindex]), \
        False, False) 
    
    if syncdb['state'] != 'success':
        print general.writeOutErrorLog(serviceName, syncdb['info'])

# checker : confirm api data
try:                 
    apidata = urllib.urlopen(\
                            general.apiInfo['protocol'] \
                            + general.apiInfo['host'] \
                            + general.apiInfo['port'] \
                            + general.apiInfo['path'] \
                            + general.apiInfo['ev']
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





















