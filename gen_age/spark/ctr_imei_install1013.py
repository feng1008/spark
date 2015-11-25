#!/usr/bin/python
#-*- coding:utf8 -*-
import datetime
from pyspark import SparkContext, SparkConf
import sys
from operator import add
from string import translate
from string import punctuation

feature=set(["gender", "imei", "yob"])

imei_dict=None

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
        for t in lines.strip('\r\n').split('&'):
            t_sta=t.split('=')[0]
            if t_sta not in feature:
                continue
            if t_sta=='imei':
                imei=t.split('=')[1]
                if imei=='':
                    continue
            elif t_sta=='gender':
                gender=t.split('=')[1]
                if gender not in ['F', 'M']:
                    continue
                gender='1' if gender=='F' else '0'
            else:
                yob=t.split('=')[1]
                if not yob.isdigit() or int(yob)<10 or int(yob)>70:
                    continue
        return (imei, gender+'\t'+yob)
    except:
        return ("", "")

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

    if ei_str not in imei_dict.value or ei_str=='':
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

def temp_map(lines):
    t=lines.strip('\r\n').split('\t')
    return (t[0], t[1]+'\t'+t[2])

def main(sc):
    global imei_dict

    imei_file=sys.argv[1]
    install_file=sys.argv[2]
    output_file=sys.argv[3]
    temp_path=sys.argv[4]
    # imei=dict(sc.textFile(imei_file).map(parse_imei).filter(tuple_filter).collect())
    # imei_dict=sc.broadcast(imei)
    # print len(imei)

    sc.textFile(imei_file).map(parse_imei).filter(tuple_filter).map(map_string).saveAsTextFile(temp_path)
    imei=sc.textFile(temp_path).map(temp_map).collect()
    imei_dict=sc.broadcast(imei)

    data=sc.textFile(install_file).map(parse_install).filter(tuple_filter).reduceByKey(packageReduce).map(map_string)
    data.saveAsTextFile(output_file)

    sc.stop()

if __name__=='__main__':
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    main(sc)