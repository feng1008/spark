#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
from operator import add

ALPHA=0.75
predict_dict=None
app_wei_dict=None

def null_filter(lines):
    if lines==[] or lines=="":
        return False
    return True

def tuple_filter(lines):
    if lines[0]=="" or lines[1]=="":
        return False
    return True

def parse_pre_dict(text):
    t=text.strip('\r\n').split('\t')
    return (t[0], t[1])

def parse_data(text):
    imei, package_str=text.strip('\r\n').split('\t')
    result=[]
    if not predict_dict.value.has_key(imei):
        return result
    weight=predict_dict.value[imei]
    for app in package_str.split('|'):
        result.append((app, weight))
    return result

def app_reduce(a, b):
    return a+'|'+'b'

def app_map(text):
    imei, weight=text.strip('\r\n').split('\t')
    count=0
    wei_sum=0.0
    for wei in weight.split('|'):
        count+=1
        wei_sum+=float(wei)
    if count==0:
        return ("", "")
    return (imei, wei_sum/count)

def user_map(text):
    imei, package_str=text.strip('\r\n').split('\t')
    count=0
    app_sum=0.0
    for app in package_str.split('|'):
        count+=1
        app_sum+=float(app_wei_dict.value[app])
    score=ALPHA*predict_dict.value[imei]+(1-ALPHA)*(app_sum/count)
    return imei+'\t'+str(score)

def main(sc):
    pre_dict=sys.argv[1]
    user_file=sys.argv[2]
    output_file=sys.argv[3]

    first_predict_dict=dict(sc.textFile(pre_dict).map(parse_pre_dict))
    predict_dict=sc.broadcast(first_predict_dict)

    user_data=sc.textFile(user_file)
    app_weight=dict(user_data.flatMap(parse_data).filter(null_filter).reduceByKey(app_reduce).map(app_map))
    app_wei_dict=sc.broadcast(app_weight)

    result=user_data.map(user_map)
    result.saveAsTextFile(output_file)

if __name__=='__main__':
    sc=SparkContext()
    main(sc)