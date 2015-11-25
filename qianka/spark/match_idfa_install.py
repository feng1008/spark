#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
from operator import add
from string import translate
from string import punctuation

idfa_set=None

def string_filter(lines):
    if lines=='':
        return False
    return True

def tupleFilter(line):
    if line[0]=='' or line[1]=='':
        return False
    else:
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

def parse_idfa(lines):
    idfa=lines.strip(',"\r\n').replace('-', '').lower()
    if is_IDFA(idfa):
        return idfa
    return ""

def parse(text):
    try:
        key_str, ei_str, mac_str, rate_str, cid_str, ifa_str, udid_str, nid_str, le_str, pkgs_str=text.strip('\r\n').split('\t')
    except:
        return ("", "")

    if ifa_str not in idfa_dict.value or ifa_str=='':
        return ("", "")
    temp_dict=eval(pkgs_str)
    result=''

    for t in temp_dict.keys():
        if t.translate(None, punctuation).lower().isalnum():
            result+=t+'|'

    if result!='':
        return (ifa_str, result[:-1])
    else:
        return ("", "")

def packageReduce(a, b):
    t_a=a.strip('\r\n').split('|')
    t_b=b.strip('\r\n').split('|')
    t_list=list(set(t_a+t_b))
    
    result=''
    for x in t_list:
        result+=x+'|'
    return result[:-1]

def main(sc):
    global idfa_dict

    user_install_file=sys.argv[1]
    idfa_file=sys.argv[2]
    outputFile=sys.argv[3]

    idfa=set(sc.textFile(idfa_file).map(parse_idfa).filter(string_filter).collect())
    idfa_dict=sc.broadcast(idfa)

    user_package=sc.textFile(user_install_file).map(parse).filter(tupleFilter).reduceByKey(packageReduce).map(lambda x:x[0]+'\t'+x[1])
    user_package.saveAsTextFile(outputFile)
    sc.stop()

if __name__=='__main__':
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    main(sc)