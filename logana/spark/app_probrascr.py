#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
from operator import add
from string import translate
from string import punctuation

features=set(["ei", "prv", "pns", "mac", "ifa"])

aos_set=None
ios_set=None

def list_filter(lines):
    if len(lines)==0:
        return False
    return True

def string_filter(lines):
    if lines=="":
        return False
    return True

def parse(text):
    global aos_set
    global ios_set

    # try:
    result=[]
    for ele in text.strip('\r\n').split('&'):
        attr, value=ele.split('=')
        if attr not in features:
            continue
        elif attr=='ei':
            ei=value
        elif attr=='ifa':
            ifa=value
        elif attr=='mac':
            mac=value
        elif attr=='prv':
            prv=value
        elif attr=='pns':
            pns=value
            apps=pns.split('|')
        else:
            continue
    if ifa=='' and ei=='' and mac=='' or apps==[] or prv=='':
        return []
    if ifa!='':
        os='ios'
        app_set=ios_set
    elif ei!='':
        os='android'
        app_set=aos_set
    elif mac!='':
        os='mac'
        app_set=aos_set

    for ap in apps:
        if ap in aos_set.value:
            result.append((ap+'\t'+os, prv))
    return result
    # except:
    #     return []

def cat_reduce(a, b):
    return a+'\t'+b

def map_string(lines):
    return lines[0]+'\t'+str(lines[1])

def pro_map(lines):
    d={}
    result=lines[0]+'\t'
    for pro in lines[1].split('\t'):
        if pro in d:
            d[pro]+=1
        else:
            d[pro]=1
    if len(d)==0:
        return ""
    listd=sorted(d.items(), key=lambda x:x[1], reverse=True)
    for k, v in listd:
        result+=k+':'+str(v)+'|'
    return result[:-1]

def app_map(lines):
    if lines!='':
        return lines.strip('\r\n')
    return ""

def aos_filter(lines):
    if lines[0].split('\t')[1]!='android' and lines[0].split('\t')[1]!='mac':
        return False
    return True

def ios_filter(lines):
    if lines[0].split('\t')[1]!='ios':
        return False
    return True

def main(sc):
    global aos_set
    global ios_set

    aos_file=sys.argv[1]
    ios_file=sys.argv[2]
    input_file=sys.argv[3]
    aos_output_file=sys.argv[4]
    ios_output_file=sys.argv[5]

    aos_app=set(sc.textFile(aos_file).map(app_map).filter(string_filter).collect())
    aos_set=sc.broadcast(aos_app)

    ios_app=set(sc.textFile(ios_file).map(app_map).filter(string_filter).collect())
    ios_set=sc.broadcast(ios_app)

    # data=sc.textFile(input_file).flatMap(parse).filter(list_filter).reduceByKey(cat_reduce).map(pro_map).filter(string_filter)
    data=sc.textFile(input_file).flatMap(parse).filter(list_filter).reduceByKey(cat_reduce)

    aos_data=data.filter(aos_filter).map(pro_map).filter(string_filter)
    aos_data.saveAsTextFile(aos_output_file)

    ios_data=data.filter(ios_filter).map(pro_map).filter(string_filter)
    ios_data.saveAsTextFile(ios_output_file)
    sc.stop()

if __name__=="__main__":
    sc=SparkContext()
    main(sc)