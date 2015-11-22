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
        ymid, id_class, first_str, second_str=text.strip('\r\n').split('\t');
    except:
        return ("","");

    if first_str=='' or second_str=='':
        return ("","");

    first_class, first_inter, normal_first_inter=first_str.split(':');
    first_inter=LAST5_DECAY_RATIO*float(first_inter);
    normal_first_inter=my_sigmoid(first_inter);
    re_first_str=first_class+':'+str(first_inter)+':'+str(normal_first_inter);

    re_second_str='';
    for s_str in second_str.split('|'):
        if len(s_str.strip('\r\n').split(':'))==4:
            second_class_t, second_class_c, second_inter, normal_second_inter=s_str.split(':');
            second_class=second_class_t+':'+second_class_c;
        else:
            continue;
        second_inter=LAST5_DECAY_RATIO*float(second_inter);
        normal_second_inter=my_sigmoid(second_inter);
        re_second_str+=second_class+':'+str(second_inter)+':'+str(normal_second_inter)+'|'
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
            first_class, second_class=appDict.value[app+'\t'+platform].split('\t');
        else:
            continue;

        if first_class!=u'购物电商':
            continue;
        first_num+=1;

        for s_c in second_class.split('|'):
            if s_c==u'无':
                continue;
            if sec_dict.has_key(s_c):
                sec_dict[s_c]+=1;
            else:
                sec_dict[s_c]=1;
    if first_num==0.0:
        return ("", "");
    n_first_num=my_sigmoid(first_num);
    result+=u'购物电商'+':'+str(first_num)+':'+str(n_first_num)+'\t';

    if len(sec_dict)!=0:
        for key,values in sec_dict.items():
            n_values=my_sigmoid(values);
            result+=key+':'+str(values)+':'+str(n_values)+'|';
    result=result[:-1];

    return (ymid+'\t'+id_class, result);

def strReduce(a,b):
    a1,a2=a.split('\t');
    b1,b2=b.split('\t');
    return a1+'|'+b1+'\t'+a2+'|'+b2;

def mapInterest(text):
    ymid_id_class=text[0];
    first_str, second_str=text[1].split('\t');

    first_num=0.0;
    for f_s in first_str.split('|'):
        try:
            first_num+=float(f_s.split(':')[1]);
        except:
            continue;

    n_first_num=my_sigmoid(first_num);

    result=ymid_id_class+'\t'+u'购物电商'+':'+str(first_num)+':'+str(n_first_num)+'\t';

    second_dict={};

    for s_str in second_str.strip('\r\n').split('|'):
        if len(s_str.split(':'))==4:
            s_class_t, s_class_c, s_num, s_n_num=s_str.split(':');
            s_class=s_class_t+':'+s_class_c;
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

    lastData=sc.textFile(lastFile).map(parseLast).filter(tupleFilter2);
    # lastData.map(lambda x:x[0]+'\t'+x[1]).saveAsTextFile('last');
    todayData=sc.textFile(inputFile).map(parse).filter(tupleFilter).map(mapCategory).filter(tupleFilter2)
    # todayData.map(lambda x:x[0]+'\t'+x[1]).saveAsTextFile('today');

    result=lastData.union(todayData).reduceByKey(strReduce).map(mapInterest);
    result.saveAsTextFile(outputFile);
    sc.stop();

if __name__=="__main__":
    conf=SparkConf();
    sc=SparkContext(conf=conf);
    main(sc);
