#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
from operator import add
from string import translate
from string import punctuation

app_set=None

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
    global app_set
    try:
        ymid, user_str=text.strip('\r\n').split("\t")
        if ymid=="":
            return []
        result=[]
        for lines in user_str.split('&'):
            id_class, login_time, packages=lines.split('^')
            for app in packages.split('|'):
                if app in app_set.value:
                    result.append((app, 1))
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
    global app_set

    app_file=sys.argv[1]
    input_file=sys.argv[2]
    idfa_output_file=sys.argv[3]

    apps=set(sc.textFile(app_file).map(app_map).filter(string_filter).collect())
    app_set=sc.broadcast(apps)

    idfa_data=sc.textFile(input_file).flatMap(parse).filter(list_filter).reduceByKey(add).map(map_string)
    idfa_data.saveAsTextFile(idfa_output_file)
    sc.stop()

if __name__=="__main__":
    sc=SparkContext()
    main(sc)