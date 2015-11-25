#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
from operator import add

def tuple_filter(lines):
    if lines[0]=='' or lines[1]=='':
        return False
    else:
        return True

def string_filter(lines):
    if lines=="":
        return False
    else:
        return True

def parse(text):
    try:
        ymid, user_str=text.strip('\r\n').split("\t")
        if ymid=="":
            return ("","")
        package_lines=user_str.split('&')
        app_packages=set()
        id_class=''
        result=[]
        for lines in package_lines:
            id_class, login_time, packages=lines.split('^')
            if id_class=='':
                continue
            apps=set(packages.split('|'))
            app_packages|=apps
        for app in app_packages:
            if len(app.split(' '))>1:
                continue
            result.append((id_class+'\t'+app,1))
        return result
    except:
        return ""

def app_map(lines):
    return lines[0].split('\t')[-1]+'\t'+str(lines[1])

def idfa_filter(lines):
    if lines[0].startswith('idfa'):
        return True
    else:
        return False

def imei_filter(lines):
    if lines[0].startswith('imei'):
        return True
    else:
        return False

def main(sc):
    input_file=sys.argv[1]
    imei_output_file=sys.argv[2]
    idfa_output_file=sys.argv[3]

    all_data=sc.textFile(input_file).flatMap(parse).filter(string_filter).reduceByKey(add)
    imei_data=all_data.filter(imei_filter).map(app_map).sortBy(lambda x:int(x.split('\t')[-1]), ascending=False)
    idfa_data=all_data.filter(idfa_filter).map(app_map).sortBy(lambda x:int(x.split('\t')[-1]), ascending=False)

    imei_data.saveAsTextFile(imei_output_file)
    idfa_data.saveAsTextFile(idfa_output_file)

if __name__=="__main__":
    sc=SparkContext()
    main(sc)