# -*- coding: utf-8 -*-
"""
author : JianKai Wang
description : send info to the specific line group
date : Dec 2017
Line Notify : https://notify-bot.line.me
document : 
* line sticker : https://devdocs.line.me/files/sticker_list.pdf
Notice :
* Add the official account (LINE Notify) into the group 
  receiving the notification.
Requirement:
* Configuration Content Format
```
[Name_1]
token = 1234567890abcdefghijklmnopqrstuvwxyz1234567

[Name_2]
token = 1234567890abcdefghijklmnopqrstuvwxyz1234567
```
Uasge :
python main.py <configuration> <token_name> <message> [<sticker>|<image>]
python LineNotify.py -c configuration.txt -n Dev -m "msg"
python LineNotify.py -c configuration.txt -n Dev -m "msg" -p 1 -d 114
python LineNotify.py -c configuration.txt -n Dev -m "msg" -i data/img.png

Exit :
0 - Complete
1 - Lose necessary parameters or not found. (Necessary: conf, token_name, message)
2 - Either sticker package id or sticker id is error.
3 - Image is not found.
99- Unexpected error
"""

import sys
import os
import ConfigParser
import getopt
import requests

class SendInfoToLineGroup():
    
    __confPath = ""
    __envName = ""
    __message = ""
    __conf = ""
    __token = ""
    __state = {}

    def __setStatus(self, state, info):
        self.__state = {"state" : state, "info" : info}
    
    def __loadConfFile(self):
        if os.path.isfile(self.__confPath):
            config = ConfigParser.ConfigParser()
            config.read(self.__confPath)
            if len(config.sections()) > 0:
                self.__setStatus(0, "Load the configuration file successfully.")
                self.__conf = config
        else:
            self.__setStatus(-1, "Fail to load the configuration file.")
            
    def __AccessToken(self):
        if self.__state['state'] != 0:
            return
        self.__token = self.__conf.get(self.__envName,"token")
        self.__setStatus(0, "Load the toekn successfully.")
        
    def __init__(self, confPath, envName, msg):
        self.__confPath = confPath
        self.__envName = envName
        self.__message = msg     
        self.__conf = {}
        self.__token = ""
        self.__state = {}

        # initialization
        self.__loadConfFile()
        self.__AccessToken()
        
    def getToken(self):
        if self.__state['state'] == 0:
            return(self.__token)
        else:
            return(self.__state['info'])
        
    def sendMsgByOriginPackage(self, getMsg):
        url = 'https://notify-api.line.me/api/notify'
        headers = {
            'Authorization': 'Bearer ' + self.__token, 
            'Content-Type' : 'application/x-www-form-urlencoded'
        }
        payload = {'message': getMsg}
        r = requests.post(url, headers = headers, params = payload)
        return r
        
    def sendMsgAndStickerByOriPkg(self, getMsg, getStickerPkgId, getStickerId):
        url = "https://notify-api.line.me/api/notify"
        headers = {
            "Authorization": "Bearer " + self.__token
        }
        payload = {\
            "message": getMsg, \
            "stickerPackageId": getStickerPkgId, \
            'stickerId': getStickerId
        }
        r = requests.post(url, headers = headers, params = payload)
        return r
        
    def sendMsgAndImgByOriPkg(self, getMsg, getPicUri):
        url = "https://notify-api.line.me/api/notify"
        headers = {
            "Authorization": "Bearer " + self.__token
        }
        payload = {'message': getMsg}
        files = {'imageFile': open(getPicUri, 'rb')}
        r = requests.post(url, headers = headers, params = payload, files = files)
        return r

try:
    opts, args = getopt.getopt(sys.argv[1:], "c:n:m:p:d:i:",\
          ["conf=","name=","msg=","stkpkg=","stkid=","img="])
    allOpts = {opts[i][0]: opts[i][1] for i in range(0, len(opts), 1)}
except getopt.GetoptError as err:
    sys.exit(1)

# -----------------------------------------------------------------------------
# input check    
# -----------------------------------------------------------------------------
configPath = ""
if "-c" in allOpts.keys() and os.path.isfile(allOpts["-c"]):
    configPath = allOpts["-c"]
elif "--conf" in allOpts.keys() and os.path.isfile(allOpts["--conf"]):
    configPath = allOpts["--conf"]
else:
    sys.exit(1)
    
envName = ""
if "-n" in allOpts.keys() and len(allOpts["-n"]) > 0:
    envName = allOpts["-n"]
elif "--name" in allOpts.keys() and len(allOpts["--name"]) > 0:
    envName = allOpts["--name"]
else:
    sys.exit(1)

msg = ""
if "-m" in allOpts.keys() and len(allOpts["-m"]) > 0:
    msg = allOpts["-m"]
elif "--msg" in allOpts.keys() and len(allOpts["--msg"]) > 0:
    msg = allOpts["--msg"]
else:
    sys.exit(1)

stkPkg = 0
stkId = 0
if "-p" in allOpts.keys() \
    and int(allOpts["-p"]) > 0 and int(allOpts["-p"]) < 5:
    stkPkg = int(allOpts["-p"])
elif "--stkpkg" in allOpts.keys() \
    and int(allOpts["--stkpkg"]) > 0 and int(allOpts["--stkpkg"]) < 5:
    stkPkg = int(allOpts["--stkpkg"])

if stkPkg != 0 and "-d" in allOpts.keys() \
    and int(allOpts["-d"]) > 0 and int(allOpts["-d"]) < 130:
    stkId = int(allOpts["-d"])
elif stkPkg != 0 and "--stkid" in allOpts.keys() \
    and int(allOpts["--stkid"]) > 0 and int(allOpts["--stkid"]) < 130:
    stkId = int(allOpts["--stkid"])
elif stkPkg != 0:
    sys.exit(2)

imgPath = ""
if "-i" in allOpts.keys() and os.path.isfile(allOpts["-i"]):
    imgPath = allOpts["-i"]
elif "--img" in allOpts.keys() and os.path.isfile(allOpts["--img"]):
    imgPath = allOpts["--img"]
    
# -----------------------------------------------------------------------------
# send the info
# -----------------------------------------------------------------------------
lineApiObj = SendInfoToLineGroup(configPath, envName, msg)

# get the token
#print(lineApiObj.getToken())

# send the info
retState = {}
if stkPkg > 0 and stkId > 0:
    # send the sticker
    retState = lineApiObj.sendMsgAndStickerByOriPkg(msg, stkPkg, stkId)
elif len(imgPath) > 0:
    # send the image
    retState = lineApiObj.sendMsgAndImgByOriPkg(msg, imgPath)
else:
    # only send the message
    retState = lineApiObj.sendMsgByOriginPackage(msg)

if retState.status_code == 200:
    sys.exit(0)
else:
    #print(sys.exc_info())
    sys.exit(99)











