#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys

imei_dict=None

def tuple_filter(lines):
    if lines[0]=="" or lines[1]="":
        return False
    return True

def string_filter(lines):
    if lines=="":
        return False
    return True

def parse_log(lines):
    feature_set=set(("imei", "os", "yob", "gender"))
    for ele in lines.strip('\r\n').split('&'):
        if ele.split('=')[0] not in feature_set:
            continue
        if ele.startswith("imei"):
            imei=ele.split('=')[-1]
        elif ele.startswith("os"):
            os=ele.split('=')[-1]
        elif ele.startswith("yob"):
            yob=ele.split('=')[-1]
        elif ele.startswith("gender"):
            gender=ele.split('=')[-1]
        else:
            continue
    if imei=='' or os!='android' or gender not in ['M', 'F']:
        return ("", "")
    return (imei, yob+'\t'+gender)

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
            return ei_str+'\t'+imei_dict.value[ei_str]+'\t'+result[:-1]
        else:
            return ""
    except:
        return ""

def main(sc):
    log_file=sys.argv[1]
    install_file=sys.argv[2]
    output_file=sys.argv[3]

    data_dict=dict(sc.textFile(log_file).map(parse_log).filter(tuple_filter).collect())
    imei_dict=sc.broadcast(data_dict)

    pack_data=sc.textFile(install_file).map(parse).filter(string_filter)
    pack_data.saveAsTextFile(output_file)
    sc.stop()

if __name__=="__main__":
    sc=SparkContext()
    main(sc)