#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf;
from operator import add;
import sys;
import codecs;
import json;
import math;
import os;

appDict=None;

def parse(text):
    try:
        ymid, package_str=text.strip('\r\n').split("\t");
        if ymid=="":
            return ("","");
        package_lines=package_str.split('&');
        app_packages=[];
        for lines in package_lines:
            id_class, login_time, packages=lines.split('^');
            if id_class=="" or login_time=="":
                continue;
            apps=packages.split('|');
            app_packages+=apps;

        return (ymid+'\t'+id_class, app_packages);
    except:
        return ("","");

def parseDict(text):
    t=text.split('\t');
    if len(t)!=4:
        return ("","");
    return (t[0]+'\t'+t[1], t[2]+'\t'+t[3]);


def tupleFilter(oneTuple):
    if oneTuple[0]=="" or oneTuple[1]=="":
        return False;
    else:
        return True;

def strFilter(line):
    t=line.split('\t');
    if len(t)<3:
        return False;
    else:
        return True;

def mapCategory(text):
    ymid, id_class=text[0].split('\t');
    if id_class=='idfa':
        platform='iOS';
    elif id_class=='imei' or id_class=='mac':
        platform='Android';
    else:
        platform='unknown';

    app_list=text[1];
    fir_dict={};
    sec_dict={};
    result=ymid+'\t'+id_class;
    for app in app_list:
        if appDict.value.has_key(app+'\t'+platform):
            firsr_class, second_class=appDict.value[app+'\t'+platform].split('\t');
        else:
            continue;

        if firsr_class!=u'购物电商' or second_class==u'无':
            continue;

        # firsr_class, second_class=appDict.value[app+'\t'+platform].split('\t');
        # try:
        #     firsr_class, second_class=appDict.value[app+'\t'+platform].split('\t');
        #     # print firsr_class.encode('utf-8');
        # except:
        #     continue;
        # print firsr_class+'\t'+second_class;
        for f_c in firsr_class.split('|'):
            if fir_dict.has_key(f_c):
                fir_dict[f_c]+=1;
            else:
                fir_dict[f_c]=1;

        for s_c in second_class.split('|'):
            if sec_dict.has_key(s_c):
                sec_dict[s_c]+=1;
            else:
                sec_dict[s_c]=1;

    if len(fir_dict)!=0:
        result+='\t';
        # first_sum=float(sum(fir_dict.values()));
        for key,values in fir_dict.items():
            # n_values=values/first_sum;
            n_values=1.0/(1+math.exp(-values));
            result+=key+':'+str(values)+':'+str(n_values)+'|';
        result=result[:-1];

    if len(sec_dict)!=0:
        result+='\t';
        # second_sum=sum(sec_dict.values());
        for key,values in sec_dict.items():
            # n_values=values/second_sum;
            n_values=1.0/(1+math.exp(-values));
            result+=key+':'+str(values)+':'+str(n_values)+'|';
        result=result[:-1];

    return result;

def main(sc):
    global appDict;

    inputFile=sys.argv[1];
    dictFile=sys.argv[2];
    outputFile=sys.argv[3];

    # generateDict(dictFile);

    interest_dict = dict(sc.textFile(dictFile).map(parseDict).collect());
    # interest_dict = sc.textFile(dictFile).map(parseDict);

    appDict=sc.broadcast(interest_dict);

    # result=sc.textFile(inputFile).map(parse).filter(tupleFilter).reduceByKey(add).map(mapCategory).filter(strFilter);
    result=sc.textFile(inputFile).map(parse).filter(tupleFilter).reduceByKey(add).map(mapCategory).filter(strFilter);
    result.saveAsTextFile(outputFile);

    sc.stop();

if __name__=="__main__":
    conf=SparkConf();
    sc=SparkContext(conf=conf);
    main(sc);