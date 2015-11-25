#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
from operator import add
from string import translate
from string import punctuation

def list_filter(lines):
    if len(lines)==0:
        return False
    else:
        return True

def parse(text):
    try:
        key_str, ei_str, mac_str, rate_str, cid_str, ifa_str, udid_str, nid_str, le_str, pkgs_str=text.strip('\r\n').split('\t')

        if ei_str=='':
            return []
        temp_dict=eval(pkgs_str)
        result=[]

        for t in temp_dict.keys():
            if int(temp_dict[t]['Lastedit'])<1427817600:
                continue
            if t.translate(None, punctuation).lower().isalnum():
                result.append((t,1))

        return result
    except:
        return []

def app_map(lines):
    return lines[0]+'\t'+str(lines[1])

def main(sc):
    input_file=sys.argv[1]
    imei_output_file=sys.argv[2]
#    idfa_output_file=sys.argv[3]

    imei_data=sc.textFile(input_file).flatMap(parse).filter(list_filter).reduceByKey(add).sortBy(lambda x:int(x[-1]), ascending=False).map(app_map)
#    imei_data=all_data.filter(imei_filter).map(app_map).sortBy(lambda x:int(x.split('\t')[-1]), ascending=False)
#    idfa_data=all_data.filter(idfa_filter).map(app_map).sortBy(lambda x:int(x.split('\t')[-1]), ascending=False)

    imei_data.saveAsTextFile(imei_output_file)
#    idfa_data.saveAsTextFile(idfa_output_file)
    sc.stop()

if __name__=="__main__":
    sc=SparkContext()
    main(sc)