#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
from operator import add
import os
import pickle

positive_dict=None
negative_dict=None


def tupleFilter(line):
    if line[0]=="" or line[1]=="":
        return False
    else:
        return True

def parse(text):
    try:
        package_str=text.strip('\r\n')
        result=[]
        for app in package_str.split(' '):
            result.append((app, 1))
        return result
    except:
        return ("","")

def NB_classify(line):
    # try:
    rate=1.0
    ratio=len(positive_dict.value)/float(len(negative_dict.value))
    package_str=line.strip('\r\n')
    result=[]
    for app in package_str.split(' '):
        if positive_dict.value.has_key(app):
            positive_count=int(positive_dict.value[app])
        else:
            positive_count=1

        if negative_dict.value.has_key(app):
            negative_count=int(negative_dict.value[app])
        else:
            negative_count=1

        rate*=ratio*negative_count/float(positive_count)
    rate=rate/ratio
    if rate>1.0:
        #negative
        return 'negative:'+'\t'+'rate:'+str(rate)+", is not an high quality user!"
    else:
        return 'positive:'+'\t'+'rate:'+str(rate)+", is an high quality user!"
    # except:
    #     return "uncorrect package format"

def main(sc):
    global positive_dict
    global negative_dict

    positive_file=sys.argv[1]
    negative_file=sys.argv[2]
    test_filte=sys.argv[3]
    result_file=sys.argv[4]

    if os.path.exists('positive_dict'):
        posi_dict=sc.textFile('positive_dict').map(lambda x:(x.split('\t')[0], x.split('\t')[1]))
    else:
        posi_dict=sc.textFile(positive_file).flatMap(parse).filter(tupleFilter).reduceByKey(add)
        p_dict=dict(posi_dict.collect())
        pickle.dump(p_dict, open('p_dict.pkl', 'w'))
        p_min=min(p_dict.values())
        p_max=max(p_dict.values())
        print str(p_max)
        p_range=float(p_max-p_min)
        posi_dict.map(lambda x:x[0]+'\t'+str(int(2+100*(x[1]+1-p_min)/p_range))).saveAsTextFile('positive_dict')
    positive_dict=sc.broadcast(dict(posi_dict.collect()))

    if os.path.exists('negative_dict'):
        nega_dict=sc.textFile('negative_dict').map(lambda x:(x.split('\t')[0], x.split('\t')[1]))
    else:
        nega_dict=sc.textFile(negative_file).flatMap(parse).filter(tupleFilter).reduceByKey(add)
        n_dict=dict(nega_dict.collect())
        pickle.dump(n_dict, open('n_dict.pkl', 'w'))
        n_min=min(n_dict.values())
        n_max=max(n_dict.values())
        print str(n_max)
        n_range=float(n_max-n_min)
        nega_dict.map(lambda x:x[0]+'\t'+str(int(2+100*(x[1]+1-p_min)/p_range))).saveAsTextFile('negative_dict')
    negative_dict=sc.broadcast(dict(nega_dict.collect()))

    # positive_dict=dict(posi_dict.collect())
    # negative_dict=dict(nega_dict.collect())

    sc.textFile(test_filte).map(NB_classify).saveAsTextFile(result_file)
    sc.stop()

if __name__=='__main__':
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    main(sc)