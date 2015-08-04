#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf;
import time;
import sys;
import os;

appDict=None;

def strFilter(line):
    t=line.split('\t');
    if len(t)<3:
        return False;
    else:
        return True;

def tupleFilter(line):
    if line[0]=="" or line[1]=="":
        return False;
    else:
        return True;

def parseLast(text):
    t=text.split('\t');
    if len(t)==4:
        ymid, id_class, first_class, second_class=t;
        return (ymid+'\t'+id_class, first_class+'\t'+second_class);
    elif len(t)==3:
        ymid, id_class, first_class=t;
        return (ymid+'\t'+id_class,first_class);
    else:
        return ("","");

def parseDict(text):
    t=text.split('\t');
    if len(t)!=4:
        return ("","");
    return (t[0]+'\t'+t[1], t[2]+'\t'+t[3]);

def parse(text):
    ymid, package_str=text.strip('\r\n').split("\t");
    if ymid=="":
        return "";
    package_lines=package_str.split('&');
    first_dict={};
    second_dict={};
    tuple_key='';
    result='';

    for lines in package_lines:
        id_class, login_time, packages=lines.split('^');
        tuple_key=ymid+'\t'+id_class;
        if id_class=="" or login_time=="":
            continue;

        if id_class=='idfa':
            platform='iOS';
        elif id_class=='imei' or id_class=='mac':
            platform='Android';
        else:
            platform='unknown';
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

            if first_class!=u'购物电商' or second_class==u'无':
                continue;

            if first_dict.has_key(first_class):
                if first_dict[first_class]<last_time:
                    first_dict[first_class]=last_time;
            else:
                first_dict[first_class]=last_time;
            
            if second_dict.has_key(first_class):
                if second_dict[second_class]<last_time:
                    second_dict[second_class]=last_time;
            else:
                second_dict[second_class]=last_time;

    for key,values in first_dict.items():
        result+=key+':'+str(values)+'|';
    result=result[:-1]+'\t';
    for key,values in second_dict.items():
        result+=key+':'+str(values)+'|';
    result=result[:-1];
    return (tuple_key, result);

def activeReduce(a,b):
    ta=a.split('\t');
    first_dict={};
    second_dict={};
    if len(ta)==2:
        first_a, second_a=ta;
    elif len(ta)==1:
        first_a=a;second_a='';

    tb=b.split('\t');
    if len(tb)==2:
        first_b, second_b=tb;
    elif len(tb)==1:
        first_b=b;second_b='';

    first_str=first_a+'|'+first_b;
    second_str=second_a+'|'+second_b;

    for first_t in first_str.split('|'):
        try:
            k, v=first_t.split(':');
        except:
            continue;
        if first_dict.has_key(k):
            if int(first_dict[k])<int(v):
                first_dict[k]=v;
            else:
                continue;
        else:
            first_dict[k]=v;

    for second_t in second_str.split('|'):
        try:
            k, v=second_t.split(':');
        except:
            continue;
        if second_dict.has_key(k):
            if int(second_dict[k])<int(v):
                second_dict[k]=v;
            else:
                continue;
        else:
            second_dict[k]=v;

    result='';
    if len(first_dict)!=0:
        for key, values in first_dict.items():
            result+=key+':'+values+'|';
        result=result[:-1]+'\t';

    if len(second_dict)!=0:
        for key, values in second_dict.items():
            result+=key+':'+values+'|';
    result=result[:-1];
    return result;

def tupleMap(line):
    return line[0]+'\t'+line[1];

def main(sc):
    global appDict;

    inputFile=sys.argv[1];
    dictFile=sys.argv[2];
    lastFile=sys.argv[3];
    outputFile=sys.argv[4];    
        
    active_dict = dict(sc.textFile(dictFile).map(parseDict).collect());
    appDict=sc.broadcast(active_dict);

    lastData=sc.textFile(lastFile).map(parseLast).filter(tupleFilter);
    # lastData.saveAsTextFile('last');

    todayData=sc.textFile(inputFile).map(parse).filter(tupleFilter);
    # todayData=sc.textFile(inputFile).map(parse).filter(tupleFilter).map(tupleMap);
    # todayData.saveAsTextFile(outputFile);

    result=lastData.union(todayData).reduceByKey(activeReduce).filter(lambda x: False if x=='' else True).map(tupleMap);
    result.saveAsTextFile(outputFile);
    sc.stop();

if __name__=='__main__':
    conf=SparkConf();
    sc=SparkContext(conf=conf);
    main(sc);
