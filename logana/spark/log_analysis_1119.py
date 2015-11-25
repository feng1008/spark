#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys
from operator import add
from string import translate
from string import punctuation

NUM=2000
features=set(["drt","ei", "prv", "pns", "mac", "ifa"])
app_top_set=None
cid_dict=None

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
    global app_top_set
    global cid_dict

    result=[]

    os, device_id=lines[0].split('\t')

    if lines[0] not in cid_dict.value:
        return result
    device, height, width=cid_dict.value[lines[0]].split('\t')
    
    for tus in lines[1].split('&'):
        drt, prv, pns=tus.split('\t')
        for app in pns.split('|'):
            if os+'\t'+app in app_top_set.value:
                if drt!='':
                    result.append(('drt'+'\t'+drt+'\t'+os+'\t'+app,1))
                if prv!='':
                    result.append(('prv'+'\t'+prv+'\t'+os+'\t'+app,1))
                if device!='':
                    result.append(('device'+'\t'+device+'\t'+os+'\t'+app,1))
                if height!='' and width!='':
                    result.append(('hei_wid'+'\t'+height+'\t'+width+'\t'+os+'\t'+app,1))
                result.append(('app'+'\t'+os+'\t'+app, 1))

    result=list(set(result))
    return result

def filter_isaos(lines):
    if lines[0].split('\t')[0]=='android':
        return True
    return False

def filter_isios(lines):
    if lines[0].split('\t')[0]=='ios':
        return True
    return False

def map_top_app(lines):
    result=[]

    app_list=[]
    try:
        os=lines[0].split('\t')[0]
        for tus in lines[1].split('&'):
            apps=tus.split('\t')[-1]
            app_list.extend(apps.split('|'))
    except:
        return []
    app_list=list(set(app_list))
    for app in app_list:
        result.append((os+'\t'+app,1))
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
    key=lines[0].split('\t')[1:]
    return '\t'.join(key)+'\t'+str(lines[1])

def main(sc):
    global app_top_set
    global cid_dict

    cid_file=sys.argv[1]
    log_file=sys.argv[2]
    prv_output=sys.argv[3]
    drt_output=sys.argv[4]
    device_output=sys.argv[5]
    hei_wid_output=sys.argv[6]
    app_output=sys.argv[7]
    # all_output=sys.argv[3]

    cid_data=dict(sc.textFile(cid_file).map(parse_cid).filter(list_filter).collect())
    cid_dict=sc.broadcast(cid_data)

    log_data=sc.textFile(log_file).map(parse).filter(tuple_filter).reduceByKey(device_reduce, 1000)
    
    app_set_data=log_data.flatMap(map_top_app).filter(list_filter).reduceByKey(add)
    aos_set=app_set_data.filter(filter_isaos).sortBy(lambda x:x[1],ascending=False).map(lambda x:x[0]).take(NUM)
    ios_set=app_set_data.filter(filter_isios).sortBy(lambda x:x[1],ascending=False).map(lambda x:x[0]).take(NUM)
    app_top_set=sc.broadcast(set(aos_set+ios_set))

    all_data=log_data.flatMap(map_bran_scr).filter(tuple_filter)

    all_data.filter(filter_prv).reduceByKey(add, 1000).map(map_tuple2str).saveAsTextFile(prv_output)
    all_data.filter(filter_drt).reduceByKey(add, 1000).map(map_tuple2str).saveAsTextFile(drt_output)
    all_data.filter(filter_device).reduceByKey(add, 1000).map(map_tuple2str).saveAsTextFile(device_output)
    all_data.filter(filter_hei_wid).reduceByKey(add, 1000).map(map_tuple2str).saveAsTextFile(hei_wid_output)
    all_data.filter(filter_app).reduceByKey(add, 1000).map(map_tuple2str).saveAsTextFile(app_output)

    sc.stop()

if __name__=="__main__":
    sc=SparkContext()
    main(sc)