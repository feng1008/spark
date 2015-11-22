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
    if line[0]=="" or len(line[1].strip('\r\n').split('\t'))<2:
        return False;
    else:
        return True;

def parseLast(text):
    t=text.split('\t');
    try:
        ymid, id_class, first_class, second_class=text.split('\t');
        return (ymid+'\t'+id_class, first_class+'\t'+second_class);
    except:
        return ("","");

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

            if first_class!=u'购物电商':
                continue;

            if first_class_time<last_time:
                first_class_time=last_time;
            
            for s_class in second_class.split('|'):
                # if s_class==u'无':
                #     continue;
                if second_dict.has_key(s_class):
                    if second_dict[s_class]<last_time:
                        second_dict[s_class]=last_time;
                else:
                    second_dict[s_class]=last_time;

    result+=u'购物电商'+':'+str(first_class_time)+'\t';

    for key,values in second_dict.items():
        result+=key+':'+str(values)+'|';
    result=result[:-1];
    return (tuple_key, result);

def activeReduce(a,b):
    first_class_time=0;
    second_dict={};

    first_a, second_a=a.strip('\r\n').split('\t');
    first_b, second_b=b.strip('\r\n').split('\t');

    if int(first_a.split(':')[1])<int(first_b.split(':')[1]):
        first_class_time=int(first_b.split(':')[1]);
    else:
        first_class_time=int(first_a.split(':')[1]);

    second_str=second_a+'|'+second_b;

    for second_t in second_str.split('|'):
        try:
            k1, k2, v=second_t.split(':');
            k=k1+':'+k2;
        except:
            continue;
        if second_dict.has_key(k):
            if int(second_dict[k])<int(v):
                second_dict[k]=int(v);
            else:
                continue;
        else:
            second_dict[k]=int(v);

    result=u'购物电商'+':'+str(first_class_time)+'\t';

    if len(second_dict)!=0:
        for key, values in second_dict.items():
            result+=key+':'+str(values)+'|';
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
    # todayData.saveAsTextFile(outputFile);

    result=lastData.union(todayData).reduceByKey(activeReduce).map(tupleMap);
    result.saveAsTextFile(outputFile);
    sc.stop();

if __name__=='__main__':
    conf=SparkConf();
    sc=SparkContext(conf=conf);
    main(sc);