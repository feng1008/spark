#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf;
from operator import add;
import operator;
import sys;
import os;
import math;

appDict=None;

def tupleFilter(line):
    if line[0]=="" or line[1]=="":
        return False;
    else:
        return True;

def parseLast(text):
    result=[];
    try:
        ymid, id_class, first_str, second_str=text.strip('\r\n').split('\t');
    except:
        return ("","");

    first_class, first_inter, normal_first_inter=first_str.split(':');
    first_inter=0.95*float(first_inter);
    normal_first_inter=1.0/(1+math.exp(-first_inter));
    re_first_str=first_class+':'+str(first_inter)+':'+str(normal_first_inter);

    re_second_str='';
    for s_str in second_str.split('|'):
        try:
            second_class, second_inter, normal_second_inter=s_str.split(':');
        except:
            continue;
        second_inter=0.95*float(second_inter);
        normal_second_inter=1.0/(1+math.exp(-second_inter));
        re_second_str+=second_class+':'+second_inter+':'normal_second_inter+'|'
    return (ymid+'\t'+id_class, re_first_str+'\t'+re_second_str[:-1]);

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

def mapCategory(text):
    ymid, id_class=text[0].split('\t');
    if id_class=='idfa':
        platform='iOS';
    elif id_class=='imei' or id_class=='mac':
        platform='Android';
    else:
        platform='unknown';

    app_list=text[1];
    first_num=0.0;
    sec_dict={};
    result='';
    for app in app_list:
        if appDict.value.has_key(app+'\t'+platform):
            firsr_class, second_class=appDict.value[app+'\t'+platform].split('\t');
        else:
            continue;

        if firsr_class!=u'购物电商' or second_class==u'无':
            continue;
        first_num+=1;

        for s_c in second_class.split('|'):
            if sec_dict.has_key(s_c):
                sec_dict[s_c]+=1;
            else:
                sec_dict[s_c]=1;

    n_first_num=1.0/(1+math.exp(-first_num));
    result+=u'购物电商'+':'+str(first_num)+':'+str(n_first_num)+'\t';

    if len(sec_dict)!=0:
        # second_sum=sum(sec_dict.values());
        for key,values in sec_dict.items():
            # n_values=values/second_sum;
            n_values=1.0/(1+math.exp(-values));
            result+=key+':'+str(values)+':'+str(n_values)+'|';
        result=result[:-1];

    return (ymid+'\t'+id_class, result);

def strReduce(a,b):
    a1,a2=a.split('\t');
    b1,b2=b.split('\t');
    return a1+'&'+b1+'\t'+a2+'&'+b2;

def recalculateDict(data_str,first=True):
    t_dict={}
    temp=data_str.strip('\r\n').split('&');
    for i in range(len(temp)):
        items=temp[i].split('|');
        for its in items:
            t=its.split(':');
            if len(t)==3:
                t0,t1,t2=its.split(':');
                t_key=t0;
                t_value=t1;
            else:
                t0,t1,t2,t3=its.split(':');
                t_key=t0+':'+t1;
                t_value=t2;
            if i==0:            
                t_dict[t_key]=0.9*float(t_value);
            else:
                if t_dict.has_key(t_key):
                    t_dict[t_key]=float(t_dict[t_key])+float(t_value);
                else:
                    t_dict[t_key]=float(t_value);
    return t_dict;

def mapInterest(text):
    ymid_id_class=text[0];
    first_str, second_str=text[1].split('\t');
    first_dict={};
    second_dict={};
    # ymid, id_class, first_str, second_str=text.split('\t');
    result=ymid_id_class+'\t';

    first_dict=recalculateDict(first_str);
    second_dict=recalculateDict(second_str);

    # first_sum=sum(first_dict.values());
    # second_sum=sum(second_dict.values());

    for key,values in first_dict.items():
        # n_values=values/float(first_sum);
        n_values=1.0/(1+math.exp(-values));
        result+=key+':'+str(values)+':'+str(n_values)+'|';
    result=result[:-1]+'\t';

    for key,values in second_dict.items():
        # n_values=values/float(second_sum);
        n_values=1.0/(1+math.exp(-values));
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

    lastData=sc.textFile(lastFile).map(parseLast);
    # lastData.saveAsTextFile('last');
    todayData=sc.textFile(inputFile).map(parse).filter(tupleFilter).map(mapCategory).filter(tupleFilter);
    # todayData.saveAsTextFile('today');

    result=lastData.union(todayData).reduceByKey(strReduce).map(mapInterest);
    result.saveAsTextFile(outputFile);
    sc.stop();

if __name__=="__main__":
    conf=SparkConf();
    sc=SparkContext(conf=conf);
    main(sc);
