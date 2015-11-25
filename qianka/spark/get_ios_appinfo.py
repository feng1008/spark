#/usr/bin/python
#-*- coding:utf8 -*-
"""
To get app infio

Author: libaichuan@youmi.net
Data: 2015/6/1
CopyRight@Youmi 2015
"""

import sys
import base64
import hashlib
from string import punctuation
import urllib
import json
import time

def getfrom_api(packages):
    myurl = "https://itunes.apple.com/lookup?bundleId=" + packages +"&country=cn"
    urlText = urllib.urlopen(myurl)
    content = urlText.read()
    #tmp = json.loads(content)
    return content.replace(':false',':0').replace(':true',':1').replace('\r\n','').replace('\n','').strip()

def get_json_info():
    """
    Get json info from API.
    """
    package_list = []
    istream = open(sys.argv[1],"r")
    for line in istream:
        content = line.strip('\r\n').split('\t')
        package_name = content[0]
        if package_name.strip() != "":
            package_list.append(package_name.strip())

    sys.stderr.write("total packages: " + str(len(package_list))+"\n")
    tmp_list = []
    appstore_out = open(sys.argv[1]+"_appstore_out.txt","w")
    count = 0
    for i in range(len(package_list)):
        tmp_list.append(package_list[i])
        if len(tmp_list) == 50:
            try:
                appstore_rv = getfrom_api(','.join(tmp_list))
                appstore_out.write(appstore_rv+"\n")
            except:
                continue
            del tmp_list[:]
            appstore_out.flush()
            count += 1
            sys.stderr.write(time.strftime("%Y%m%d-%H:%M:%S") + " " + str(count*50) + "\n")
            time.sleep(3)
    #last batch
    appstore_rv = getfrom_api(','.join(tmp_list))
    appstore_out.write(appstore_rv+"\n")
    appstore_out.close()
    istream.close()

def read_json_info():
    """
    Extract category info from API results.
    """
    for line in sys.stdin:
        try:
            result = eval(line.strip('\r\n'))
        except:
            continue
        count = int(result["resultCount"])
        if count < 1:
            continue
        for ele in result["results"]:
            trackName = ele["trackName"]
            bundleId = ele["bundleId"]
            trackId = str(ele["trackId"])
            genres = ele["genres"] #list
            primaryGenreName = ele["primaryGenreName"]

            print bundleId + "\t" + trackName + "\t" + trackId + "\t" + "|".join(genres) #+ "\t" + primaryGenreName

def get_category():
    """
    Extract apps with specified category
    """
    category_name = sys.argv[1]
    for line in sys.stdin:
        line = line.strip('\r\n')
        content = line.split('\t')
        if content[-1] == category_name:
            print line

def get_top200k_category_info():
    """
    Get top200k category info, order by frequency
    Usage: cat top200k_file | python *.py wandoujia_file yingyongbao_file > outfile
    """
    as_path = sys.argv[1]

    as_dict = {}
    istream = open(as_path, "r")
    for line in istream:
        content = line.strip('\r\n').split('\t',1)
        as_dict[content[0]] = content[1]
    istream.close()

    for line in sys.stdin:
        line = line.strip('\r\n')
        content = line.split('\t',1)
        key = content[0].strip()
        value = "" + "\t" + "" + "\t" + ""
        if key in as_dict:
            value = as_dict[key]
        newline = line + "\t" + value
        assert(len(newline.split("\t"))==5)
        print newline

if __name__ == "__main__":
    get_json_info()
    # read_json_info()
    # get_top200k_category_info()
    #print getfrom_api("com.tencent.,com.tencent.xin")
