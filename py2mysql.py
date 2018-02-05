# -*- coding: utf-8 -*-

"""
#
# author : jiankaiwang (http://welcome-jiankaiwang.rhcloud.com/)
# source code in github : seed (https://github.com/jiankaiwang/seed)
# document in gitbook : seed (https://www.gitbook.com/book/jiankaiwang/seed/details)
#

# desc : function to print key-value query data
def dictRes(a):
    for item in range(0, len(a['data']), 1):
        for key in range(0, len(a['data'][item].keys()), 1):
            print a['data'][item].keys()[key], a['data'][item].values()[key]

# desc : function to print query data without key
def nonDictRes(a):
    for item in range(0, len(a['data']), 1):
        for each in range(0, len(a['data'][item]), 1):
            print a['data'][item][each]

# desc : connect to the mysql server
py2my = py2mysql("127.0.0.1", "3306", "user", "password", "database_name")        
print py2my.checkConnectionValid()  

# desc : query data
queryData = py2my.execsql("select * from table where year = %s limit 10;", (2000,), True, True)
dictRes(queryData)

# desc : insert data
insertData = py2my.execsql("insert into table (year, week, value) values (%s, %s, %s);", (2000, 1, 'value'), False, False)    
print insertData

# desc : update data
updateData = py2my.execsql("update table set value = %s where year = %s and week = %s;", ('new_value', 2000, 1), False, False)    
print updateData

# desc : delete data
deleteData = py2my.execsql("delete from table where year = %s and week = %s;", (2000, 1), False, False)    
print deleteData
"""

import mysql.connector

class py2mysql:
    
    # ----------
    # private
    # ----------
    __host = ""
    __port = ""
    __user = ""
    __pass = ""
    __dbname = ""
    __connectionValid = False
    __msg = ""
    
    #
    # desc : return status
    # retn : { "state" : [success|failure|warning], "info" : "message", "data" : []}
    #
    def __retStatus(self, state, info, data):
        return {"state" : state, "info" : info, "data" : data}
    
    #
    # desc : check mysql server connection
    #
    def __checkConnect(self):
        try:
            conn = mysql.connector.connect(\
                                          host = self.__host, \
                                          port = self.__port, \
                                          user = self.__user, \
                                          password = self.__pass, \
                                          database = self.__dbname\
                                          )
            self.__connectionValid = True
            conn.close()
        except mysql.connector.Error as err:
            self.__connectionValid = False
            self.__msg = "{}".format(err)
    
    
    # ----------
    # public
    # ----------
    
    # 
    # desc : constructor
    #
    def __init__(self, host, port, user, pwd, dbname):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__pass = pwd
        self.__dbname = dbname
        self.__connectionValid = False
        self.__msg = ""
    
        # check connect
        self.__checkConnect()
    
    #
    # desc : get conection status
    #
    def checkConnectionValid(self):
        if self.__connectionValid == False:
            return self.__retStatus("failure", self.__msg, "")
        else :
            return self.__retStatus("success", "Connection is valid.", "")
            
    #
    # desc : execute sql command
    # inpt :
    # |- sqlCmd : "SELECT first_name, hire_date FROM employees WHERE hire_date BETWEEN %s AND %s"
    # |- parameterInSeq (tuple) : (datetime.date(1999, 1, 1), datetime.date(1999, 12, 31))
    # |- isQueryFlag : {True|False}
    # |- asdict (return as dictionary) : {True|False}
    #
    def execsql(self, sqlCmd, parameterInSeq, isQueryFlag, asdict=True):
        if self.__connectionValid == False:
            return self.__retStatus("failure", self.__msg, "")
            
        if not (isinstance(sqlCmd, str) \
           and isinstance(parameterInSeq, tuple) \
           and isinstance(isQueryFlag, bool)\
           and isinstance(asdict, bool))\
           :
            return self.__retStatus("failure", "Parameters passed are wrong.", "")
            
        # connection is valid
        try:
            conn = mysql.connector.connect(\
                                  host = self.__host, \
                                  port = self.__port, \
                                  user = self.__user, \
                                  password = self.__pass, \
                                  database = self.__dbname\
                                  )
        
            cursor = conn.cursor()            
            cursor.execute(sqlCmd, parameterInSeq)
            
            if isQueryFlag:
                
                curInfo = [desc[0] for desc in cursor.description]
                rawData = cursor.fetchall()
                
                retData = []

                if asdict:
                    tmp = {}
                    for item in range(0, len(rawData), 1):
                        tmp = {}
                        for col in range(0, len(curInfo), 1):
                            tmp.setdefault(curInfo[col], rawData[item][col])
                        retData.append(tmp)
                else:
                    retData.append(curInfo)
                    tmp = []
                    for item in range(0, len(rawData), 1):
                        tmp = []
                        for col in range(0, len(curInfo), 1):
                            tmp.append(rawData[item][col])
                        retData.append(tmp)
                
                return self.__retStatus("success", "Complete query.", retData)
                
            else:
                
                conn.commit();
                
                return self.__retStatus("success", "Complete non-query sql command.", "")
                
            cursor.close()
            conn.close()
                
        except mysql.connector.Error as err:
            return self.__retStatus("failure", "{}".format(err), "")
        
        









        
        
        
        