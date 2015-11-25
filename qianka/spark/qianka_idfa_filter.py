#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
import urllib
from operator import add
from string import translate
from string import punctuation

pack_num_dict=None
idfa_set=None

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

def getfrom_api(packages):
    myurl = "https://itunes.apple.com/lookup?bundleId=" + packages +"&country=cn"
    urlText = urllib.urlopen(myurl)
    content = urlText.read()
    return content.replace(':false',':0').replace(':true',':1').replace('\r\n','').replace('\n','').strip()

def parse(text):
    try:
        key_str, ei_str, mac_str, rate_str, cid_str, ifa_str, udid_str, nid_str, le_str, pkgs_str=text.strip('\r\n').split('\t')

        if ifa_str=='' or ifa_str not in idfa_set.value:
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
    except:
        return ("", "")

def packageReduce(a, b):
    t_a=a.strip('\r\n').split('|')
    t_b=b.strip('\r\n').split('|')
    t_list=list(set(t_a+t_b))
    
    result=''
    for x in t_list:
        result+=x+'|'
    return result[:-1]

def getPrice(line):
    t=line.split('\t')
    if len(t)==2:
        idfa, pack_str=line.split('\t')
    else:
        return ""
    result=idfa
    for packs in pack_str.strip('\r\n').split('\t'):
        try:
            package=packs.split(':')[0]
        except:
            continue

        content=getfrom_api(package)
        try:
            price=float(eval(content)['results'][0]['price'])
        except:
            continue
        if price==0.0:
            continue
        result+='\t'+package
    return result

def main(sc):
    global pack_num_dict
    global idfa_set

    user_install_file=sys.argv[1]
    idfa_file=sys.argv[2]
    outputFile=sys.argv[3]

    idfa=set(sc.textFile(idfa_file).map(lambda x:x.strip('\r\n').split('\t')[0].replace('-','').lower()).collect())
    idfa_set=sc.broadcast(idfa)

    user_package=sc.textFile(user_install_file).map(parse).filter(tupleFilter).reduceByKey(packageReduce).map(lambda x:x[0]+'\t'+x[1])
    user_package.saveAsTextFile(outputFile)
    sc.stop()

if __name__=='__main__':
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    main(sc)