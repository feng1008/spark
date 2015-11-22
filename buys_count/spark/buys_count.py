#!/usr/bin/python
#coding="utf-8"
from pyspark import SparkContext, SparkConf;
from operator import add;
import operator;
import sys;
from string import atoi;

#read useful data from file
def parse(text):
    try:
        lines=text.strip('\r\n').split(",");
        userId=lines[1];
        idClass=lines[0];
        itemName=lines[2];
        item_num=lines[3];

        if userId=="" or idClass=="" or itemName=="" or item_num=="":
            return ("","");
        return (itemName, userId+'\t'+idClass+'\t'+item_num);
    except:
        return ("","");

#filter uncorrect imei
def myFilter(oneTuple):
    if oneTuple[0]=="":
        return False;
    else:
        return True;

def myReduce(a,b):
    return a+'|'+b;

def flat2str(oneTuple):
    return oneTuple[0]  + "\t" + oneTuple[1]

def mapCountItem(text):
    itemName=text[0];
    good_items=text[1].split('|');
    result=[];
    item_sum=0;

    dit={};

    for itm in good_items:
        tSplit=itm.split('\t');
        userId=tSplit[0];
        idClass=tSplit[1];
        # try:
        item_num=int(tSplit[2]);
        # except:
        #     # print 'dfs'+tSplit[2];
        #     continue;
        # item_num=atoi(tSplit[2]);
        if dit.has_key(userId+'\t'+idClass):
            dit[userId+'\t'+idClass]+=item_num;
        else:
            dit[userId+'\t'+idClass]=item_num;
        item_sum+=item_num;

    for key,value in dit.items():
        result.append((key, itemName+'\t'+str(value)+'\t'+str(value/float(item_sum))));
    return result;

def mapResult(text):
    user_key=text[0];
    good_items=text[1].split('|');
    result=user_key;
    good_list=[];
    for goods in good_items:
        t_good=goods.split('\t');
        itemName=t_good[0];
        itemNum=t_good[1];
        item_ratio=t_good[2];
        good_list.append((itemName, int(itemNum), item_ratio));

    good_temp=sorted(good_list, key=operator.itemgetter(1),reverse=True);

    for goods in good_temp:
        itemName=goods[0];
        itemNum=str(goods[1]);
        item_ratio=goods[2];
        result+='\t'+itemName+':'+itemNum+':'+item_ratio;
    return result;

# def mapItemName(text):
#     tSplit=text[0].split('\t');
#     userId=tSplit[0];
#     idClass=tSplit[1];
#     itemName=tSplit[2];
#     num=text[1];
#     return (itemName, str(num)+'&&'+userId+'\t'+idClass+'\t'+str(num));

# def reduceItemName(a,b):
#     a_num=atoi(a.split('&&')[0]);
#     b_num=atoi(b.split('&&')[0]);
#     return str(a_num+b_num)+'&&'+a.split('&&')[1]+'|'+b.split('&&')[1];

if __name__=="__main__":
    inputFile=sys.argv[1];
    outputFile=sys.argv[2];

    conf=SparkConf();
    sc=SparkContext(conf=conf);

    result=sc.textFile(inputFile).map(parse).filter(myFilter).reduceByKey(myReduce).flatMap(mapCountItem).reduceByKey(myReduce).map(mapResult)
    # .flatMap(mapCountItem).reduceByKey(myReduce).map(mapResult);
    result.saveAsTextFile(outputFile);
    sc.stop();

