#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
from operator import add
from string import translate
from string import punctuation

aos_set=None
ios_set=None

def list_filter(lines):
    if len(lines)==0:
        return False
    else:
        return True

def string_filter(lines):
    if lines=="":
        return False
    return True

def parse(text):
    global aos_set
    global ios_set

    try:
        ymid, user_str=text.strip('\r\n').split("\t")
        if ymid=="":
            return []
        result=[]
        cls=''
        app_packages=set()
        for lines in user_str.split('&'):
            id_class, login_time, packages=lines.split('^')
            if id_class=='imei':
                cls='Android:'
            elif id_class=='idfa':
                cls='ios:'
            else:
                return []
            apps=set(packages.split('|'))
            app_packages|=apps
        for app in app_packages:
            if app in aos_set.value and cls=='Android:' or app in ios_set.value and cls=='ios:':
                result.append((cls+'\t'+app, 1))
        return result
    except:
        return []

def map_string(lines):
    return lines[0]+'\t'+str(lines[1])

def app_map(lines):
    if lines!='':
        return lines.strip('\r\n')
    return ""

def main(sc):
    global aos_set
    global ios_set

    aos_file=sys.argv[1]
    ios_file=sys.argv[2]
    input_file=sys.argv[3]
    idfa_output_file=sys.argv[4]

    aos_app=set(sc.textFile(aos_file).map(app_map).filter(string_filter).collect())
    aos_set=sc.broadcast(aos_app)

    ios_app=set(sc.textFile(ios_file).map(app_map).filter(string_filter).collect())
    ios_set=sc.broadcast(ios_app)

    data=sc.textFile(input_file).flatMap(parse).filter(list_filter).reduceByKey(add, 1).map(map_string)
    data.saveAsTextFile(idfa_output_file)
    sc.stop()

if __name__=="__main__":
    sc=SparkContext()
    main(sc)