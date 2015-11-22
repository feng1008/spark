#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
import urllib
from operator import add
from string import translate
from string import punctuation

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
        # key, imei, mac, rate, x, y, z, cid, last_edit, packages=text.strip('\r\n').split('\t')
        key_str, ei_str, mac_str, rate_str, cid_str, ifa_str, udid_str, nid_str, le_str, pkgs_str=text.strip('\r\n').split('\t')

        if ei_str=='' or ei_str not in imei_dict.value:
            return ("", "")
        temp_dict=eval(pkgs_str)
        result=''

        for t in temp_dict.keys():
            if t.translate(None, punctuation).lower().isalnum():
                result+=t+'|'

        if result!='':
            return (ei_str+'\t'+imei_dict.value[ei_str], result[:-1])
        else:
            return ("", "")
    except:
        return ("", "")

def parse_imei(text):
    device_id, os, gender, age=text.strip('\r\n').split('\t')
    if os !='android':
        return ("", "")
    return (device_id, gender+'\t'+age)

def packageReduce(a, b):
    t_a=a.strip('\r\n').split('|')
    t_b=b.strip('\r\n').split('|')
    t_list=list(set(t_a+t_b))
    
    result=''
    for x in t_list:
        result+=x+'|'
    return result[:-1]

def main(sc):
    global imei_dict

    user_install_file=sys.argv[1]
    imei_file=sys.argv[2]
    outputFile=sys.argv[3]

    imei=dict(sc.textFile(imei_file).map(parse_imei).filter(tupleFilter).collect())
    imei_dict=sc.broadcast(imei)

    user_package=sc.textFile(user_install_file).map(parse).filter(tupleFilter).reduceByKey(packageReduce).map(lambda x:x[0]+'\t'+x[1])
    user_package.saveAsTextFile(outputFile)
    sc.stop()

if __name__=='__main__':
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    main(sc)