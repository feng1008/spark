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
    if len(lines)==0:
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
        return (ifa+'\t'+os, drt+'\t'+prv+'\t'+pns)
    elif ei!='':
        os='android'
        return (ei+'\t'+os, drt+'\t'+prv+'\t'+pns)
    elif mac!='':
        os='android'
        return (mac+'\t'+os, drt+'\t'+prv+'\t'+pns)
    else:
        return ("", "") 
    # except:
    #     return []

def device_reduce(a, b):
    return a+'&'+b

def device_map(lines):
    device_id, os=lines[0].strip('\r\n').split('\t')
    result=[]

    for tus in lines[1].split('&'):
        t0, t1, t2=tus.split('\t')
        apps=t2.split('|')
        for ap in apps:
            result.append((os+'\t'+ap+'\t'+'drt'+'\t'+t0, 1))
            result.append((os+'\t'+ap+'\t'+'prv'+'\t'+t1, 1))
            result.append((os+'\t'+ap+'\t'+'single', 1))

        tups=[(apps[i],apps[j]) for i in range(len(apps)) for j in range(i+1, len(apps))]
        for t in tups:
            if t[0]<t[1]:
                result.append(('pairs'+'\t'+os+'\t'+t[0]+'\t'+t[1], 1))
            else:
                result.append(('pairs'+'\t'+os+'\t'+t[1]+'\t'+t[0], 1))
    result=list(set(result))
    return result

def map_string(lines):
    return lines[0]+'\t'+lines[1]

def pro_filter(lines):
    if lines[0].strip('\r\n').split('\t')[2]=='prv':
        return True
    return False

def drt_filter(lines):
    if lines[0].strip('\r\n').split('\t')[2]=='drt':
        return True
    return False

def single_filter(lines):
    if lines[0].strip('\r\n').split('\t')[2]=='single':
        return True
    return False

def pairs_filter(lines):
    if lines[0].strip('\r\n').split('\t')[0]=='pairs':
        return True
    return False

def tuple_zero_filter(lines):
    if lines[0]=="" or lines[1]=='' or lines[1]<0.1:
        return False
    return True

def num_filter(lines):
    if lines[1]<100:
        return False
    return True

def single_map(lines):
    t=lines[0].strip('\r\n').split('\t')
    return (t[0]+'\t'+t[1], lines[1])

def pairs_map(lines):
    t=lines[0].strip('\r\n').split('\t')
    return (t[1]+'\t'+t[2]+'\t'+t[3], lines[1])

def pairs_map_ratio(lines):
    global app_dict

    os, app1, app2=lines[0].strip('\r\n').split('\t')
    # if os+'\t'+p1 not in app_dict.value:
    #     app1, app2=p2, p1
    # else:
    #     app1, app2=p1, p2
    dict_len=len(app_dict.value)
    if os+'\t'+app1 not in app_dict.value or os+'\t'+app2 not in app_dict.value:
        return ("", "")

    app1_num=int(app_dict.value[os+'\t'+app1])
    app2_num=int(app_dict.value[os+'\t'+app2])
    ratio=float('%.2f'%(dict_len*float(lines[1])/(app1_num*app2_num)))
    result_key=os+'\t'+app1+':'+str(app1_num)+'\t'+app2+':'+str(app2_num)+'\t'+'union_num:'+str(lines[1])+'\t'+'sum_count:'+str(dict_len)
    return (result_key, ratio)

def ele_map(lines):
    os, ap, x, ele=lines[0].strip('\r\n').split('\t')
    return (os+'\t'+ap, ele+':'+str(lines[1]))

def tuple2str(lines):
    return lines[0]+'\t'+str(lines[1])

def ele_reduce(a, b):
    return a+'|'+b

def main(sc):
    global app_dict

    input_file=sys.argv[1]
    pro_output_file=sys.argv[2]
    drt_output_file=sys.argv[3]
    pairs_output_file=sys.argv[4]

    data=sc.textFile(input_file).map(parse).filter(tuple_filter).reduceByKey(device_reduce).flatMap(device_map).filter(list_filter).reduceByKey(add)

    codec="org.apache.hadoop.io.compress.GzipCodec"

    # province distribution
    data.filter(pro_filter).map(ele_map).filter(tuple_filter).reduceByKey(ele_reduce).map(map_string).saveAsTextFile(pro_output_file, codec)
    # time distribution
    data.filter(drt_filter).map(ele_map).filter(tuple_filter).reduceByKey(ele_reduce).map(map_string).saveAsTextFile(drt_output_file, codec)

    # apps=dict(data.filter(single_filter).map(single_map).reduceByKey(add).filter(num_filter).collect())
    apps=dict(data.filter(single_filter).map(single_map).reduceByKey(add).collect())
    app_dict=sc.broadcast(apps)

    # pairs_data=data.filter(pairs_filter).map(pairs_map).reduceByKey(add).map(pairs_map_ratio).filter(tuple_zero_filter).sortBy(lambda x:x[1], ascending=False).map(tuple2str)
    pairs_data=data.filter(pairs_filter).map(pairs_map).reduceByKey(add).map(pairs_map_ratio).sortBy(lambda x:x[1], ascending=False).map(tuple2str)
    pairs_data.saveAsTextFile(pairs_output_file, codec)

    sc.stop()

if __name__=="__main__":
    sc=SparkContext()
    main(sc)