#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
from operator import add
from string import translate
from string import punctuation

features=set(["drt","ei", "prv", "pns", "mac", "ifa"])
app_dict=None

def list_filter(lines):
    if lines==[]:
        return False
    return True

def string_filter(lines):
    if lines=="":
        return False
    return True

def tuple_filter(lines):
    if lines[0]=="" or lines[1]=="":
        return False
    return True

def parse(text):
    # try:
    result=[]
    ei='';drt='';ifa='';mac='';prv='';pns='';apps=[]
    for ele in text.strip('\r\n').split('&'):
        try:
            attr, value=ele.split('=')
        except:
            return ("", "")
        if attr not in features or value=="":
            continue
        elif attr=='ei':
            ei=value
        elif attr=='drt':
            drt=value
        elif attr=='ifa':
            ifa=value
        elif attr=='mac':
            mac=value
        elif attr=='prv':
            prv=value
        elif attr=='pns':
            pns=value
        else:
            continue
    if ifa=='' and ei=='' and mac=='' or pns=='' or prv=='' or drt=='':
        return ("", "")

    try:
        drt=drt.strip('\r\n').split('+')[1].split(':')[0]
    except:
        return ("", "")
    if ifa!='':
        os='ios'
        return (os+'\t'+ifa, drt+'\t'+prv+'\t'+pns)
    elif ei!='':
        os='android'
        return (os+'\t'+ei, drt+'\t'+prv+'\t'+pns)
    elif mac!='':
        os='android'
        return (os+'\t'+mac, drt+'\t'+prv+'\t'+pns)
    else:
        return ("", "") 
    # except:
    #     return []

def device_reduce(a, b):
    return a+'&'+b

def parse_cid(lines):
    try:
        cid, imei, ifa, mac, andid, imsi, brand, model, height, width, os, edition, device, country, language=lines.strip('\r\n').split('\t')
    except:
        return ("", "")
    if imei=='' and ifa=='' or imei!='' and ifa!='':
        return ("", "")
    if imei!='':
        return ('android'+'\t'+imei, device+'\t'+height+'\t'+width)
    if ifa!='':
        device='apple'
        return ('ios'+'\t'+ifa, device+'\t'+height+'\t'+width)

def map_bran_scr(lines):
    result=[]

    os, device_id=lines[0].split('\t')
    device, height, width=lines[1][0].split('\t')
    
    for tus in lines[1][1].split('&'):
        drt, prv, pns=tus.split('\t')
        for app in pns.split('|'):
            result.append(('drt'+'\t'+drt+'\t'+lines[0],1))
            result.append(('prv'+'\t'+prv+'\t'+lines[0],1))
            result.append(('device'+'\t'+device+'\t'+lines[0],1))
            result.append(('hei_wid'+'\t'+height+'\t'+width+'\t'+lines[0],1))
            result.append(('app'+'\t'+app+'\t'+lines[0], 1))
    result=list(set(result))
    return result

def filter_prv(lines):
    if lines[0].split('\t')[0]=='prv':
        return True
    return False

def filter_drt(lines):
    if lines[0].split('\t')[0]=='drt':
        return True
    return False

def filter_device(lines):
    if lines[0].split('\t')[0]=='device':
        return True
    return False

def filter_hei_wid(lines):
    if lines[0].split('\t')[0]=='hei_wid':
        return True
    return False

def filter_app(lines):
    if lines[0].split('\t')[0]=='app':
        return True
    return False

def filter_all(lines):
    ele_set=['prv', 'drt', 'device', 'hei_wid', 'app']
    if lines[0].split('\t')[0] not in ele_set:
        return False
    return True

def map_tuple2str(lines):
    return lines[0]+'\t'+str(lines[1])

def main(sc):
    cid_file=sys.argv[1]
    log_file=sys.argv[2]
    # prv_output=sys.argv[3]
    # drt_output=sys.argv[4]
    # device_output=sys.argv[5]
    # hei_wid_output=sys.argv[6]
    # app_output=sys.argv[7]
    all_output=sys.argv[3]

    cid_data=sc.textFile(cid_file).map(parse_cid).filter(list_filter)
    log_data=sc.textFile(log_file).map(parse).filter(tuple_filter).reduceByKey(device_reduce)
    all_data=cid_data.join(log_data).flatMap(map_bran_scr).filter(list_filter)


    codec="org.apache.hadoop.io.compress.GzipCodec"

    all_data.filter(filter_all).reduceByKey(add).sortByKey(False).map(map_tuple2str).saveAsTextFile(all_output)
    # all_data.filter(filter_prv).reduceByKey(add, 1).map(map_tuple2str).saveAsTextFile(prv_output)
    # all_data.filter(filter_drt).reduceByKey(add, 1).map(map_tuple2str).saveAsTextFile(drt_output)
    # all_data.filter(filter_device).reduceByKey(add, 1).map(map_tuple2str).saveAsTextFile(device_output)
    # all_data.filter(filter_hei_wid).reduceByKey(add, 1).map(map_tuple2str).saveAsTextFile(hei_wid_output)
    # all_data.filter(filter_app).reduceByKey(add).map(map_tuple2str).saveAsTextFile(app_output)

    # all_data.reduceByKey(add, 1).map(map_tuple2str).saveAsTextFile('result')

    sc.stop()

if __name__=="__main__":
    sc=SparkContext()
    main(sc)