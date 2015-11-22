#!/usr/bin/python
#coding="utf-8"
from pyspark import SparkContext, SparkConf;
from operator import add;
import operator;
import sys;

#filter uncorrect imei
def formatImei(imei):
    try:
        strImei=str(imei);
        if strImei.isalnum() and len(strImei)>13 and len(strImei)<16:
            return strImei;
        else:
            return "";
    except:
        return "";

#read useful data from file
def parse(text):
    try:
        lines=text.split("&");
        imei="";
        country="";
        province="";
        city="";
        # print lines;exit();
        lines=[x.encode('utf-8') for x in lines];
        for words in lines:
            if words.startswith('ei='):
                imei=words[3:];
            elif words.startswith('country='):
                country=words[8:];
            elif words.startswith("prv="):
                province=str(words.split("=",1)[1]).strip();
            elif words.startswith("cty="):
                city=words[4:];
            else:
                continue;

        if imei=="" or country=="" or province=="" or city=="":
            return ("","");
        else:
            return (imei,country+'|'+province+'|'+city);

    except:
        return ("","");

#filter uncorrect imei
def myFilter(oneTuple):
    if oneTuple[0]=="":
        return False;
    else:
        return True;

#read a small part from the large file, for test
def reduceData(text):
    f=open('reduceData.txt','w');
    count=0;
    for lines in text:
        f.write(lines.encode('utf-8')+"\n");
        count=count+1;
        if count>400:
            return;
    f.close();

def myMap(text):
    imei=text[0];
    ctry_pro_cty=text[1].split('&');
    ctry={};
    pro={};
    cty={};

    for ele in ctry_pro_cty:
        if ctry.has_key(ele.split('|')[0]):
            ctry[ele.split('|')[0]]+=1;
        else:
            ctry.update({ele.split('|')[0]:1});
        if pro.has_key(ele.split('|')[1]):
            pro[ele.split('|')[1]]+=1;
        else:
            pro.update({ele.split('|')[1]:1});
        if cty.has_key(ele.split('|')[2]):
            cty[ele.split('|')[2]]+=1;
        else:
            cty.update({ele.split('|')[2]:1});

    maxctry=sorted(ctry.iteritems(), key=operator.itemgetter(1),reverse=True);
    maxpro=sorted(pro.iteritems(), key=operator.itemgetter(1),reverse=True);
    maxcty=sorted(cty.iteritems(), key=operator.itemgetter(1),reverse=True);

    ctry_str="";
    pro_str="";
    cty_str="";

    for lines in maxctry:
        ctry_str+=str(lines[0])+":"+str(lines[1]);
        if lines!=maxctry[-1]:
            ctry_str+="|";
    for lines in maxpro:
        pro_str+=str(lines[0])+":"+str(lines[1]);
        if lines!=maxpro[-1]:
            pro_str+="|";
    for lines in maxcty:
        cty_str+=str(lines[0])+":"+str(lines[1]);
        if lines!=maxcty[-1]:
            cty_str+="|";

    ret=ret=str(imei)+"\t"+str(maxctry[0][0])+"\t"+str(maxpro[0][0])+"\t"+str(maxcty[0][0])+"\t"+ctry_str+"\t"+pro_str+"\t"+cty_str;
    # ret=str(imei)+"\t"+str(maxctry[0])+"\t"+str(maxpro[0])+"\t"+str(maxcty[0])+"\t"+str(maxctry[0])+":"+str(maxctry[1])+"\t"+str(maxpro[0])+":"+str(maxpro[1])+"\t"+str(maxcty[0])+":"+str(maxcty[1]);
    return ret;

def add2(a,b):
    return a+'&'+b;

if __name__=="__main__":
    # inputFile="reduceData.txt";
    # outputFile="result.txt";
    inputFile=sys.argv[1];
    outputFile=sys.argv[2];

    conf=SparkConf();
    sc=SparkContext(conf=conf);

    result=sc.textFile(inputFile).map(parse).filter(myFilter).reduceByKey(add2).map(myMap);
    result.saveAsTextFile(outputFile);
    sc.stop();
