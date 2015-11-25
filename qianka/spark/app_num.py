#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
from operator import add
from string import translate
from string import punctuation

idfa_set=None

def list_filter(lines):
    if lines==[]:
        return False
    return True

def is_IDFA(idfa):
    """
    IDFA format: 36 characters, contains (uppercase) letters or digits. 4 segments
    linked by '-': '-'.join([8,4,4,4,12]), e.g., 1E2DFA89-496A-47FD-9941-DF1FC4E6484A
    UMID format: 32 lowercase characters, e.g., 1e2dfa89496a47fd9941df1fc4e6484a
    """
    try:
        new_idfa = str(idfa).translate(None, punctuation).lower()
        if len(new_idfa) == 32 and new_idfa.isalnum():
            return new_idfa
        else:
            return ""
    except:
        return ""

def parse(text):
    try:
        key_str, ei_str, mac_str, rate_str, cid_str, ifa_str, udid_str, nid_str, le_str, pkgs_str=text.strip('\r\n').split('\t')
    except:
        return []

    if ifa_str=='':
        return []
    temp_dict=eval(pkgs_str)
    result=[]

    for t in temp_dict.keys():
        if t.translate(None, punctuation).lower().isalnum():
            result.append((t, 1))
    return result

def main(sc):
    global idfa_dict

    user_install_file=sys.argv[1]
    outputFile=sys.argv[2]

    user_package=sc.textFile(user_install_file).flatMap(parse).filter(list_filter).reduceByKey(add).map(lambda x:x[0]+'\t'+str(x[1]))
    user_package.saveAsTextFile(outputFile)
    sc.stop()

if __name__=='__main__':
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    main(sc)