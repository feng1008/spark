#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
import urllib
from operator import add
from string import translate
from string import punctuation
import datetime

imei_dict=None

def tupleFilter(line):
    if line[0]=='' or line[1]=='':
        return False
    else:
        return True

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

def parse(text):
    try:
        ymid, user_str=text.strip('\r\n').split("\t")
    except:
        return ("","")

    if ymid not in imei_dict.value:
        return ("","")
    id_class=''
    result=''
    d={}
    for lines in user_str.split('&'):
        id_class, login_time, packages=lines.split('^')
        if id_class!='imei':
            return("", "")
        d[login_time]=packages.split('|')

    sorted_d=sorted(d.items(), key=lambda x:x[0], reverse=False)
    time_dict={}
    for k, v in sorted_d:
        for app in v:
            if app in time_dict:
                begin=datetime.datetime.strptime(time_dict[app], "%Y-%m-%d+%H:%M:%S")
                end=datetime.datetime.strptime(k, "%Y-%m-%d+%H:%M:%S")
                if (end-begin).seconds>1800:
                    result+=app+' '
            else:
                result+=app+' '
            time_dict[app]=k
    if result!='':
        return (ymid, result[:-1])
    return ("", "")

def parse_imei(text):
    device_id, os, gender, age=text.strip('\r\n').split('\t')
    if os !='android':
        return ("", "")
    return (device_id, gender+'\t'+age)

def packageReduce(a, b):
    t=a+' '+b
    t_list=list(set(t.split(' ')))
    
    return ' '.join(t_list)

def main(sc):
    global imei_dict

    user_last5_file=sys.argv[1]
    imei_file=sys.argv[2]
    outputFile=sys.argv[3]

    imei_d=dict(sc.textFile(imei_file).map(parse_imei).filter(tupleFilter).collect())
    imei_dict=sc.broadcast(imei_d)

    sc.textFile(user_last5_file).map(parse).filter(tupleFilter).reduceByKey(packageReduce).map(lambda x:x[0]+'\t'+x[1]).saveAsTextFile(outputFile)
    # sc.textFile(user_last5_file).map(parse).filter(tupleFilter).reduceByKey(packageReduce,1000).map(lambda x:x[0]+'\t'+x[1]).saveAsTextFile(outputFile)
    sc.stop()

if __name__=='__main__':
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    main(sc)