#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
package_set=None
app_len=0

def str_filter(lines):
    if lines=='':
        return False
    else:
        return True

def parse_positive(lines):
    global app_len

    app_list=list(package_set.value)
    one_list=[0]*(app_len.value+1)
    try:
        one_list[0]=1
        app_name, package_str=lines.strip('\r\n').split('\t')
        for app in package_str.split('\t'):
            try:
                one_list[app_list.index(app)]=1
            except:
                continue
        result=str(one_list)[1:-1].replace(',',' ')
        return result
    except:
        return ""

def parse_negative(lines):
    app_list=list(package_set.value)
    one_list=[0]*(app_len.value+1)
    try:
        app_name, package_str=lines.strip('\r\n').split('\t')
        for app in package_str.split('\t'):
            try:
                one_list[app_list.index(app)]=1
            except:
                continue
        result=str(one_list)[1:-1].replace(',',' ')
        return result
    except:
        return ""

def main(sc):
    global app_len
    global package_set

    setFile=sys.argv[1]
    positive_input=sys.argv[2]
    negative_input=sys.argv[3]
    positive_output=sys.argv[4]
    negative_output=sys.argv[5]

    package_set_temp=set(sc.textFile(setFile).map(lambda x:x.strip('\r\n')).collect())
    package_len=len(package_set_temp)
    app_len=sc.broadcast(package_len)
    package_set=sc.broadcast(package_set_temp)

    positive_data=sc.textFile(positive_input).map(parse_positive).filter(str_filter)
    positive_data.saveAsTextFile(positive_output)

    negative_data=sc.textFile(negative_input).map(parse_negative).filter(str_filter)
    negative_data.saveAsTextFile(negative_output)

if __name__=='__main__':
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    main(sc)