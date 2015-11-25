#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
import datetime
import time

fee_packages=None

def tuple_filter(line):
    if line[0]=="" or line[1]=="":
        return False
    else:
        return True

def parse_fee_set(text):
    try:
        trackId,trackName,bundleId,price,currency,artistName,releaseDate=text.strip('\r\n').split(',')
        return (bundleId)
    except:
        return ("")

def parse(text):
    try:
        key_str, ei_str, mac_str, rate_str, cid_str, ifa_str, udid_str, nid_str, le_str, pkgs_str=text.strip('\r\n').split('\t')

        if ifa_str=='':
            return ("", "")

        result=''
        temp_dict=eval(pkgs_str)

        now_time=datetime.datetime.now()
        last_time=now_time+datetime.timedelta(days=-15)
        last_time_strp=time.mktime(last_time.timetuple())

        for t in temp_dict.keys():
            # print t
            if t not in fee_packages.value:
                continue
            last_edit=temp_dict[t]["Lastedit"]
            # print last_edit
            if int(last_time_strp)<int(last_edit):
                # result+=t+':'+str(last_edit)+'|'
                result+=t+'|'
            else:
                continue
        # print result
        if result!='':
            return (ifa_str, result[:-1])
        else:
            return ("", "")

    except:
        return ("", "")

def main(sc):
    global fee_packages

    input_file=sys.argv[1]
    fee_packages_file=sys.argv[2]
    output_file=sys.argv[3]

    # x=sc.textFile(fee_packages_file).map(parse_fee_set).filter(lambda x:False if x==("") else True).collect()
    # print len(x)
    # fee_pack_set=set(x)

    fee_pack_set=set(sc.textFile(fee_packages_file).map(parse_fee_set).filter(lambda x:False if x=="" else True).collect())
    # print len(fee_pack_set)
    fee_packages=sc.broadcast(fee_pack_set)

    result=sc.textFile(input_file).map(parse).filter(tuple_filter).map(lambda x:x[0]+'\t'+x[1])
    result.saveAsTextFile(output_file)

    sc.stop()

if __name__=="__main__":
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    main(sc)