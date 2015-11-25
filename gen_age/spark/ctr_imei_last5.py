#!/usr/bin/python
#-*- coding:utf8 -*-
import datetime
from pyspark import SparkContext, SparkConf
import sys
from operator import add
from string import translate
from string import punctuation

bro_imei=None

def string_filter(lines):
    if lines=="":
        return False
    return True

def list_filter(lines):
    if lines==[]:
        return False
    return True

def tuple_filter(line):
    if line[0]=='' or line[1]=='':
        return False
    else:
        return True

def parse_imei(lines):
    try:
        t1, t2=lines.strip('\r\n').split('imei=')
        imei=t2.split('&')[0]
        return imei
    except:
        return ""  

def is_IMEI(imei):
    """
    IMEI format: 15 digits, e.g., 123456789012345
    UMID format: 15 digits, e.g., 123456789012345
    """
    try:
        new_imei = str(imei).lower()
        if new_imei.isalnum() and len(new_imei) > 13 and len(new_imei) < 16:
            return new_imei
        else:
            return ""
    except:
        return ""

def parse_last5(text):
    global bro_imei
    try:
        ymid, user_str=text.strip('\r\n').split("\t")
        if ymid=="" or ymid not in bro_imei.value:
            return []
        package_lines=user_str.split('&')
        app_packages=set()
        id_class=''
        result=[]
        for lines in package_lines:
            id_class, login_time, packages=lines.split('^')
            if id_class!='imei':
                continue
            apps=set(packages.split('|'))
            app_packages|=apps
        for app in app_packages:
            result.append((ymid+'\t'+app,1))
        return result
    except:
        return []

def app_reduce(a, b):
    d={}
    for app in a.split('|'):
        d[app]=1

    for app in b.split('|'):
        if app in d:
            d[app]+=1
        else:
            d[app]=1
    result=''
    for k, v in d.items():
        result+=k+':'+str(v)+'|'
    return result[:-1]

def user_map(lines):
    ymid, app=lines[0].split('\t')
    return (ymid, app+':'+str(lines[1]))

def user_reduce(a, b):
    return a+'|'+b

def map_string(lines):
    return lines[0]+'\t'+lines[1]

def main(sc):
    global bro_imei

    imei_file=sys.argv[1]
    last5_file=sys.argv[2]
    days_ago=int(sys.argv[3])
    output_file=sys.argv[4]
 
    # imei_set=set(sc.textFile(imei_file).map(parse_imei).filter(string_filter).distinct().collect())
    imei_set=set(sc.textFile(imei_file).map(parse_imei).filter(string_filter).distinct().collect())
    bro_imei=sc.broadcast(imei_set)

    last5_data=sc.parallelize([])
    today=datetime.date.today()
    while days_ago>=1:
        temp_day=today-datetime.timedelta(days=days_ago)
        last5_path=last5_file+temp_day.strftime("%Y-%m-%d")
        last5_data+=sc.textFile(last5_path)
        days_ago-=1

    # data=sc.textFile(last5_file).flatMap(parse_last5).filter(list_filter).reduceByKey(add).map(user_map).reduceByKey(user_reduce).map(map_string)
    data=last5_data.flatMap(parse_last5).filter(list_filter).reduceByKey(add).map(user_map).reduceByKey(user_reduce).map(map_string)
    data.saveAsTextFile(output_file)

    sc.stop()

if __name__=='__main__':
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    main(sc)