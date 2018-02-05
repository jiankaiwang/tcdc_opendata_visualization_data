# -*- coding: utf-8 -*-
"""
#
# author : jiankaiwang (https://jiankaiwang.no-ip.biz/)
# project : CKAN Visualization
# service : (None)
# note : general libraries for all service modules
#
"""

import datetime
import ConfigParser

# global variables
defaultConfigPath = "config/opendata.txt"
mysqlServerInfo = {\
                   'host' : '', \
                   'port' : '', \
                   'user' : '', \
                   'pass' : '', \
                   'dbname' : '' \
                   }
apiInfo = {\
           'protocol' : 'https://', \
           'host' : 'od.cdc.gov.tw', \
           'port' : ':443', \
           'path' : '/opendataplatform/',  \
           'ev' : '?s=enterovirus&a=v1', \
           'denguev1' : '?s=dengue&a=v1', \
           'denguev2' : '?s=dengue&a=v2', \
           'diarrheapc' : '?s=diarrheapiechart&v=a1', \
           'hivbc' : '?s=hivbc&v=a1', \
           'influlinechart' : '?s=influlinechart&v=a1' \
           }
normalizedNote = {\
                  'exec' : 'execution',\
                  'error' : 'error', \
                  'comp' : 'complete',\
                  'stmp' : 'Mark a beginning time stamp.',  \
                  'etmp' : 'Mark a ending time stamp.',\
                  'stmpflag' : "starttimestamp", \
                  'etmpflag' : 'endtimestamp', \
                  'fdflag' : 'fetchdata', \
                  'fdflagdesc' : 'Fetch data is complete.', \
                  'fderror' : 'Fetching data is failure.', \
                  'fdpcerror' : 'Fetching data is complete but data is not match.', \
                  'fdpcunexceperror' : 'Fetching data is complete but data meets a unexcepted error.', \
                  'fdcheckcomp' : 'Fetching data and checking data are complete.', \
                  'dataready' : 'Data is prepared and ready to insert into the database.', \
                  'insertintodb' : 'Data is prepared for inserting into the database.', \
                  'apicheckdesc' : 'API check is complete.', \
                  'apicheckfail' : 'API check is failure.' \
                  }

def setNormalTimeStamp(getValue):
    if (int)(getValue) < 10:
        return '0' + str(getValue)
    else:
        return str(getValue)

def getCrtTimeStamp():
    response = datetime.datetime.now()
    ymd = (str)(response.year) + '/' + (str)(setNormalTimeStamp(response.month)) + '/' + (str)(setNormalTimeStamp(response.day))
    hms = (str)(setNormalTimeStamp(response.hour)) + ':' + (str)(setNormalTimeStamp(response.minute)) + ':' + (str)(setNormalTimeStamp(response.second))
    return ymd + ' ' + hms
   
def writeOutErrorLog(service, cond):
    outputList = [getCrtTimeStamp(), service, cond]    
    return ','.join(outputList)
    
#
# desc :
# inpt :
# |- getDataDict : {'servicename' : "", 'status' : "", 'starttimestamp' : "", 'endtimestamp' : "", 'note' : ""}
# |- cmdOption : 
# retn :
# {'state' : [success|failure], 'info' : ''}
#    
def retnStateMsg(getState, getInfo):
    return {'state' : getState, 'info' : getInfo}
    
def writeStatusIntoDB(getMySQLConnector, getDataDict, cmdOption):
    
    queryData = getMySQLConnector.execsql(\
                "select * from statusRecord where servicename = %s;", \
                (getDataDict['servicename'],), True, True\
                )
    if queryData['state'] != "success":
        return retnStateMsg('failure', queryData['info'])
        
    else:
        syncdb = 0
        
        if len(queryData['data']) > 0:
            # already existing
            if cmdOption == normalizedNote['stmpflag']:
                # starttimestamp
                syncdb = getMySQLConnector.execsql(\
                "update statusRecord set status = %s, starttimestamp = %s, endtimestamp = %s, note = %s where servicename = %s;", \
                (getDataDict['status'], getDataDict['starttimestamp'], getDataDict['endtimestamp'], getDataDict['note'], getDataDict['servicename']), False, False\
                )
            elif cmdOption == normalizedNote['fdflag']:
                # fetchdataflag
                syncdb = getMySQLConnector.execsql(\
                "update statusRecord set status = %s, endtimestamp = %s, note = %s where servicename = %s;", \
                (getDataDict['status'], getDataDict['endtimestamp'], getDataDict['note'], getDataDict['servicename']), False, False\
                )
            elif cmdOption == normalizedNote['etmpflag']:
                # starttimestamp
                syncdb = getMySQLConnector.execsql(\
                "update statusRecord set status = %s, endtimestamp = %s, note = %s where servicename = %s;", \
                (getDataDict['status'], getDataDict['endtimestamp'], getDataDict['note'], getDataDict['servicename']), False, False\
                )
                
        else:
            syncdb = getMySQLConnector.execsql(\
            "insert into statusRecord (servicename, status, starttimestamp, endtimestamp, note) values (%s, %s, %s, %s, %s);", \
            (getDataDict['servicename'], getDataDict['status'], getDataDict['starttimestamp'], getDataDict['endtimestamp'], getDataDict['note']), False, False\
            )    
        
        if syncdb['state'] != 'success':
            return retnStateMsg('failure', syncdb['info'])
    
    return retnStateMsg('success', '')

# load necessary information
config = ConfigParser.ConfigParser()
config.read(defaultConfigPath)
mysqlServerInfo['host'] = config.get("mysql","server")
mysqlServerInfo['port'] = config.get("mysql","port")
mysqlServerInfo['user'] = config.get("mysql","user")
mysqlServerInfo['pass'] = config.get("mysql","password")
mysqlServerInfo['dbname'] = config.get("mysql","db")





