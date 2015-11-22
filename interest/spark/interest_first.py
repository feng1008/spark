#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf;
from operator import add;
import sys;
import math;
import os;

appDict=None;
sec_class_set=set((u"商城特卖",u"导购返利",u"海淘",u"团购",u"优惠券",u"众筹夺宝",u"二手买卖"))

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

        return (ymid+'\t'+id_class, list(app_packages));
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
    if len(t)<4:
        return False;
    else:
        return True;

def my_sigmoid(x):
    if x > 10:
        return 1;
    else:
        formatrv = '%.4f'%(1/(1.0 + math.e**(-x)));
        return formatrv;

def mapCategory(text):
    global sec_class_set
    
    ymid, id_class=text[0].strip('\r\n').split('\t');
    if id_class=='idfa':
        platform='ios';
    elif id_class=='imei' or id_class=='mac':
        platform='android';
    else:
        platform='unknown';

    app_list=text[1];
    first_num=0.0;
    sec_dict={};
    result=ymid+'\t'+id_class+'\t';
    for app in app_list:
        if appDict.value.has_key(app+'\t'+platform):
            firsr_class, second_class=appDict.value[app+'\t'+platform].split('\t');
        else:
            continue;

        first_num+=1;

        for s_c in second_class.split('|'):
            if s_c not in sec_class_set:
                continue
            if sec_dict.has_key(s_c):
                sec_dict[s_c]+=1;
            else:
                sec_dict[s_c]=1;

    n_first_num=my_sigmoid(first_num)
    result+=u'购物电商'+':'+str(first_num)+':'+str(n_first_num);

    if len(sec_dict)!=0:
        result+='\t';
        for key,values in sec_dict.items():
            n_values=my_sigmoid(values)
            result+=key+':'+str(values)+':'+str(n_values)+'|';
        result=result[:-1];
    return result;

def main(sc):
    global appDict;

    inputFile=sys.argv[1];
    dictFile=sys.argv[2];
    outputFile=sys.argv[3];

    interest_dict=dict(sc.textFile(dictFile).map(parseDict).collect());
    appDict=sc.broadcast(interest_dict);

    result=sc.textFile(inputFile).map(parse).filter(tupleFilter).reduceByKey(add).map(mapCategory).filter(strFilter);
    result.saveAsTextFile(outputFile);

    sc.stop();

if __name__=="__main__":
    conf=SparkConf();
    sc=SparkContext(conf=conf);
    main(sc);