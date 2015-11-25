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

def parse_install(text):
    try:
        key_str, ei_str, mac_str, rate_str, cid_str, ifa_str, udid_str, nid_str, le_str, pkgs_str=text.strip('\r\n').split('\t')
    except:
        return ("", "")

    if ei_str not in bro_imei.value or ei_str=='':
        return ("", "")
    temp_dict=eval(pkgs_str)
    result=''

    for t in temp_dict.keys():
        if t.translate(None, punctuation).lower().isalnum():
            result+=t+'|'

    if result!='':
        return (ei_str, result[:-1])
    else:
        return ("", "")

def packageReduce(a, b):
    t_a=a.strip('\r\n').split('|')
    t_b=b.strip('\r\n').split('|')
    t_list=list(set(t_a+t_b))
    
    result=''
    for x in t_list:
        result+=x+'|'
    return result[:-1]

def map_string(lines):
    return lines[0]+'\t'+lines[1]

def main(sc):
    global bro_imei

    imei_file=sys.argv[1]
    install_file=sys.argv[2]
    output_file=sys.argv[3]
 
    imei_set=set(sc.textFile(imei_file).map(parse_imei).filter(string_filter).distinct().collect())
    bro_imei=sc.broadcast(imei_set)

    print len(imei_set)
    # data=sc.textFile(install_file).map(parse_install).filter(tuple_filter).reduceByKey(packageReduce).map(map_string)
    # data.saveAsTextFile(output_file)

    sc.stop()

if __name__=='__main__':
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    main(sc)