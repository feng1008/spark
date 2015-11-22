#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf;
from operator import add;
import operator;
import sys;
import os;
import math;

appDict=None;
LAST5_DECAY_RATIO=0.95;

def tupleFilter(line):
    if line[0]=="" or line[1]=="":
        return False;
    else:
        return True;

def tupleFilter2(line):
    if line[0]=="" or len(line[1].strip('\r\n').split('\t'))<2 or line[1].strip('\r\n').split('\t')[1]=='':
        return False;
    else:
        return True;

def my_sigmoid(x):
    if x > 10:
        return 1;
    else:
        formatrv = '%.4f'%(1/(1.0 + math.e**(-x)));
        return formatrv;

def parseLast(text):
    result=[];
    try:
        ymid, second_str=text.strip('\r\n').split('\t');
    except:
        return ("","");

    if ymid=='' or second_str=='':
        return ("","");

    re_second_str='';
    for s_str in second_str.split('|'):
        if len(s_str.strip('\r\n').split(':'))==3:
            second_class, second_inter, normal_second_inter=s_str.split(':');
        else:
            continue;
        second_inter=LAST5_DECAY_RATIO*float(second_inter);
        normal_second_inter=my_sigmoid(second_inter);
        re_second_str+=second_class+':'+str(second_inter)+':'+str(normal_second_inter)+'|'
    return (ymid, re_second_str[:-1]);

def parse(text):
    try:
        ymid, package_str=text.strip('\r\n').split("\t");
        if ymid=="":
            return ("","");
        package_lines=package_str.split('&');
        app_packages=set();
        for lines in package_lines:
            id_class, login_time, packages=lines.split('^');
            if id_class=="" or login_time=="":
                continue;
            apps=set(packages.split('|'));
            app_packages|=apps;

        return (ymid, list(app_packages));
    except:
        return ("","");

def parseDict(text):
    t=text.strip('\r\n').split('\t');
    if len(t)!=3:
        return ("","");
    return (t[0], t[2]);

def mapCategory(text):
    ymid=text[0];

    app_list=text[1];

    sec_dict={};
    result='';
    for app in app_list:
        if appDict.value.has_key(app):
            second_class=appDict.value[app];
        else:
            continue;

        for s_c in second_class.split('|'):
            if sec_dict.has_key(s_c):
                sec_dict[s_c]+=1;
            else:
                sec_dict[s_c]=1;

    result='';

    if len(sec_dict)!=0:
        for key,values in sec_dict.items():
            n_values=my_sigmoid(values);
            result+=key+':'+str(values)+':'+str(n_values)+'|';
    result=result[:-1];

    return (ymid, result);

def strReduce(a,b):
    return a+'|'+b;

def mapInterest(text):
    ymid=text[0];
    second_str=text[1];

    result=ymid+'\t';

    second_dict={};

    for s_str in second_str.strip('\r\n').split('|'):
        if len(s_str.split(':'))==3:
            s_class, s_num, s_n_num=s_str.split(':');
        else:
            continue;
        if second_dict.has_key(s_class):
            second_dict[s_class]+=float(s_num);
        else:
            second_dict[s_class]=float(s_num);

    for key,values in second_dict.items():
        n_values=my_sigmoid(values);
        result+=key+':'+str(values)+':'+str(n_values)+'|';
    result=result[:-1];

    return result;

def main(sc):
    global appDict;

    inputFile=sys.argv[1];
    dictFile=sys.argv[2];
    lastFile=sys.argv[3];
    outputFile=sys.argv[4];

    interest_dict = dict(sc.textFile(dictFile).map(parseDict).collect());
    appDict=sc.broadcast(interest_dict);

    lastData=sc.textFile(lastFile).map(parseLast).filter(tupleFilter);
    # lastData.map(lambda x:x[0]+'\t'+x[1]).saveAsTextFile('last');
    todayData=sc.textFile(inputFile).map(parse).filter(tupleFilter).map(mapCategory).filter(tupleFilter)
    # todayData.map(lambda x:x[0]+'\t'+x[1]).saveAsTextFile('today');

    result=lastData.union(todayData).reduceByKey(strReduce).map(mapInterest);
    result.saveAsTextFile(outputFile);
    sc.stop();

if __name__=="__main__":
    conf=SparkConf();
    sc=SparkContext(conf=conf);
    main(sc);
