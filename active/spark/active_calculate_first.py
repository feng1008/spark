#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf;
import time;
import sys;
import os;
import math;

appDict=None;
sec_class_set=set((u"商城特卖",u"导购返利",u"海淘",u"团购",u"优惠券",u"众筹夺宝",u"二手买卖"))

def tupleFilter(line):
    if line[0]=="" or len(line[1].strip('\r\n').split('\t'))<2:
        return False;
    else:
        return True;

def parseDict(text):
    t=text.split('\t');
    if len(t)!=4:
        return ("","");
    return (t[0]+'\t'+t[1], t[2]+'\t'+t[3]);

def parse(text):
    ymid, package_str=text.strip('\r\n').split("\t");

    if ymid=="":
        return ("","");
    package_lines=package_str.split('&');
    first_class_time=0;
    second_dict={};
    result='';
    id_class='';

    for lines in package_lines:
        id_class, login_time, packages=lines.split('^');

        if id_class=="" or login_time=="":
            continue;

        if id_class=='idfa':
            platform='ios';
        elif id_class=='imei' or id_class=='mac':
            platform='android';
        else:
            continue;

        try:
            timeArray=time.strptime(login_time, "%Y-%m-%d+%H:%M:%S");
        except:
            continue;
        last_time=int(time.mktime(timeArray));
        apps=packages.split('|');
        for app in apps:
            if appDict.value.has_key(app+'\t'+platform):
                first_class, second_class=appDict.value[app+'\t'+platform].split('\t');
            else:
                continue;

            if first_class_time<last_time:
                first_class_time=last_time;

            for s_c in second_class.split('|'):
                if s_c not in sec_class_set:
                    continue
                if second_dict.has_key(s_c):
                    if second_dict[s_c]<last_time:
                        second_dict[s_c]=last_time;
                else:
                    second_dict[s_c]=last_time;

    result+=u'购物电商'+':'+str(first_class_time)+'\t';

    for key,values in second_dict.items():
        result+=key+':'+str(values)+'|';
    result=result[:-1];
    return (ymid+'\t'+id_class, result);

def tupleMap(line):
    return line[0]+'\t'+line[1];

def main(sc):
    global appDict;

    inputFile=sys.argv[1];
    dictFile=sys.argv[2];
    outputFile=sys.argv[3];

    active_dict = dict(sc.textFile(dictFile).map(parseDict).collect());
    appDict=sc.broadcast(active_dict);

    todayData=sc.textFile(inputFile).map(parse).filter(tupleFilter).map(tupleMap);
    todayData.saveAsTextFile(outputFile);

    sc.stop();

if __name__=='__main__':
    conf=SparkConf();
    sc=SparkContext(conf=conf);
    main(sc);
