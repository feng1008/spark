#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf;
from operator import add;
import sys;
import codecs;
import json;
import numpy as np;
import os;

appDict={};

def generateDict(inputFile):
    global appDict;

    f=codecs.open(inputFile,'r','utf-8');
    for lines in f.readlines():
        try:
            name, platform, package_id, first_class, second_class, vote_num=lines.split('\t');
            if name=="" or platform=="" or package_id=="" or first_class =="" or second_class=="" or int(vote_num)<2:
                continue;
            if appDict.has_key(package_id+'\t'+platform):
                appDict[package_id+'\t'+platform]+=first_class+'\t'+second_class;
            else:
                appDict[package_id+'\t'+platform]=first_class+'\t'+second_class;
        except:
            continue;
    # json.dump(appDict,codecs.open(outputFile,'w','utf-8'),indent=4);
    f.close();

def loadDict(filename):
    global appDict;

    appDict.update(json.load(codecs.open(filename,'r','utf-8')));
    print "successfully load the dict!\n";

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

def myFilter(oneTuple):
    if oneTuple[0]=="":
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
    global appDict;

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
        try:
            firsr_class, second_class=appDict[app+'\t'+platform].split('\t');
        except:
            continue;
        # print firsr_class+'\t'+second_class;

        if fir_dict.has_key(firsr_class):
            fir_dict[firsr_class]+=1;
        else:
            fir_dict[firsr_class]=1;

        if sec_dict.has_key(second_class):
            sec_dict[second_class]+=1;
        else:
            sec_dict[second_class]=1;

    if len(fir_dict)!=0:
        result+='\t';
        # first_sum=float(sum(fir_dict.values()));
        for key,values in fir_dict.items():
            # n_values=values/first_sum;
            n_values=1.0/(1+np.exp(-values));
            result+=key+':'+str(values)+':'+str(n_values)+'|';
        result=result[:-1];
        # first_min=min(fir_dict.values());
        # first_range=float(max(fir_dict.values())-first_min);
        # for key,values in fir_dict.items():
        #     if first_range!=0.0:
        #         n_values=(values-first_min)/first_range;
        #     else:
        #         n_values=1.0/len(fir_dict);
        #     result+='\t'+key+':'+str(values)+':'+str(n_values);

    if len(sec_dict)!=0:
        result+='\t';
        # second_sum=sum(sec_dict.values());
        for key,values in sec_dict.items():
            # n_values=values/second_sum;
            n_values=1.0/(1+np.exp(-values));
            result+=key+':'+str(values)+':'+str(n_values)+'|';
        result=result[:-1];
        # second_min=min(sec_dict.values());
        # second_range=float(max(sec_dict.values())-second_min);
        # for key,values in sec_dict.items():
        #     if second_range!=0.0:
        #         n_values=(values-second_min)/second_range;
        #     else:
        #         n_values=1.0/len(sec_dict);
        #     result+='\t'+key+':'+str(values)+':'+str(n_values);

    return result;

def main(sc):
    global interest_dict;

    inputFile=sys.argv[1];
    dictFile=sys.argv[2];
    outputFile=sys.argv[3];

    generateDict(dictFile);

    result=sc.textFile(inputFile).map(parse).filter(myFilter).reduceByKey(add).map(mapCategory).filter(strFilter);
    result.saveAsTextFile(outputFile);

    # json.dump(interest_dict,codecs.open(dictoutput+'dict.json','w','utf-8'),indent=4);

    sc.stop();

if __name__=="__main__":
    conf=SparkConf();
    sc=SparkContext(conf=conf);
    main(sc);