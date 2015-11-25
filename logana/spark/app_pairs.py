#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
from operator import add

app_dict=None

def tupleFilter(line):
    if line[0]=='' or line[1]=='':
        return False
    return True

def string_filter(lines):
    if lines=='':
        return False
    return True

def parse_one(text):
    try:
        ymid, user_str=text.strip('\r\n').split("\t")
        if ymid =='':
            return []
        result=[]
        for lines in user_str.split('&'):
            id_class, login_time, packages=lines.split('^')
            if id_class=='imei':
                os='android'
            elif id_class=='idfa':
                os='ios'
            else:
                continue
            for t in packages.split('|'):
                result.append((os+'\t'+t, 1))
        return result
    except:
        return []

def parse_pairs(text):
    global app_dict

    try:
        ymid, user_str=text.strip('\r\n').split("\t")
        if ymid =='':
            return []
        result=[]
        for lines in user_str.split('&'):
            id_class, login_time, packages=lines.split('^')
            if id_class=='imei':
                os='android'
            elif id_class=='idfa':
                os='ios'
            else:
                continue
            apps=packages.split('|')
            for t in apps:
                if os+'\t'+t not in app_dict.value:
                    apps.remove(t)

            tups=[(apps[i],apps[j]) for i in range(len(apps)) for j in range(i+1, len(apps))]
            for t in tups:
                result.append((os+'\t'+t[0]+'\t'+t[1], 1))
        return result
    except:
        return []

def map_ratio(lines):
    global app_dict

    os, app1, app2=lines[0].strip('\r\n').split('\t')
    # temp=float(lines[1])
    # ratio='%.4f'%temp
    if os+'\t'+app1 not in app_dict.value or os+'\t'+app2 not in app_dict.value:
        return ("", "")
    ratio=float('%.4f'%(float(lines[1])/(float(app_dict.value[os+'\t'+app1])*float(app_dict.value[os+'\t'+app2]))))
    
    # return lines[0].strip('\r\n')+'\t'+ratio
    return (lines[0].strip('\r\n'), ratio)

def num_filter(lines):
    if lines[1]<100:
        return False
    return True

def string_zero_filter(lines):
    if lines=='' or float(lines.split('\t')[-1])==0.0:
        return False
    return True

def tuple_zero_filter(lines):
    if lines[0]=="" or lines[1]=='' or lines[1]==0.0:
        return False
    return True

def tuple_map(lines):
    return lines[0]+'\t'+str(lines[1])

def main(sc):
    global app_dict

    last5_file=sys.argv[1]
    output_file=sys.argv[2]

    apps=dict(sc.textFile(last5_file).flatMap(parse_one).filter(tupleFilter).reduceByKey(add).filter(num_filter).collect())
    app_dict=sc.broadcast(apps)

    # data=sc.textFile(last5_file).flatMap(parse_pairs).filter(tupleFilter).reduceByKey(add).map(map_ratio).filter(zero_filter)
    data=sc.textFile(last5_file).flatMap(parse_pairs).filter(tupleFilter).reduceByKey(add).map(map_ratio).filter(tuple_zero_filter).sortBy(lambda x:x[1], ascending=False, numPartitions=1).map(tuple_map)
    data.saveAsTextFile(output_file)

    sc.stop()

if __name__=='__main__':
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    main(sc)