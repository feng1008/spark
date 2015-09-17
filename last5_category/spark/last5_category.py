#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
import sys

current_catename=None
appDict=None
FIRST_CLASS_SET=set((u"金融理财",u"购物电商"))
CLASS_OUTPUT_DICT={u"金融理财":'finance',u"购物电商":'commerce'}

def null_filter(lines):
    if lines=="" or lines==[]:
        return False 
    else:
        return True

def tuple_filter(lines):
    if lines[0]=="" or lines[1]=="":
        return False
    else:
        return True

def map_class_filter(text):
    global current_catename

    ymid, id_class=text[0].split('\t')
    if id_class=='idfa':
        platform='ios'
    elif id_class=='imei' or id_class=='mac':
        platform='android'
    else:
        return ""

    package_str=text[1].strip('\r\n').split('\t')[-1]
    re_packages_str=''

    for pac_str in package_str.split('|'):
        try:
            app, login_time=pac_str.split(':')
        except:
            continue
        if appDict.value.has_key(app+'\t'+platform):
            first_class=appDict.value[app+'\t'+platform].strip('\r\n').split('\t')[0]
        else:
            continue
        if current_catename in first_class.split('|'):
            re_packages_str+=app+':'+login_time+'|'

    if re_packages_str=='':
        return ""
    else:
        return text[0]+'\t'+re_packages_str[:-1]

def parse(text):
    try:
        ymid, package_str=text.strip('\r\n').split("\t")
        if ymid=="":
            return ("","")
        package_lines=package_str.split('&')
        app_packages_dict={}
        result_key=''
        result_value=''

        for lines in package_lines:
            id_class, login_time, packages=lines.split('^')
            if id_class=="" or login_time=="":
                continue
            login_time=login_time.replace('+','*').replace(':','-')
            result_key=ymid+'\t'+id_class
            for app in packages.strip('\r\n').split('|'):
                if app_packages_dict.has_key(app):
                    app_packages_dict[app]+='+'+login_time
                else:
                    app_packages_dict[app]=login_time

        for key, value in app_packages_dict.items():
            result_value+=key+':'+value+'|'

        if result_key=='' or result_value=='':
            return ("", "")
        else:
            return (result_key, result_value[:-1])
    except:
        return ("", "")

def parse_dict(text):
    t=text.split('\t')
    if len(t)!=4:
        return ("","")
    return (t[0]+'\t'+t[1], t[2]+'\t'+t[3])

def main(sc):
    global appDict
    global current_catename

    inputFile=sys.argv[1]
    dictFile=sys.argv[2]
    output=sys.argv[3]

    interest_dict = dict(sc.textFile(dictFile).map(parse_dict).collect())
    appDict=sc.broadcast(interest_dict)

    result_all=sc.textFile(inputFile).map(parse).filter(tuple_filter)

    codec = "org.apache.hadoop.io.compress.GzipCodec"
    for f_c_key in FIRST_CLASS_SET:
        if not CLASS_OUTPUT_DICT.has_key(f_c_key):
            print "key "+f_c_key+" does not exist in CLASS_OUTPUT_DICT!"
            continue
        output_path=output+'/'+CLASS_OUTPUT_DICT[f_c_key]
        current_catename=f_c_key
        result=result_all.map(map_class_filter).filter(null_filter)
        # result.saveAsTextFile(output_path)
        result.saveAsTextFile(output_path, codec)

    sc.stop()

if __name__=="__main__":
    sc=SparkContext()
    main(sc)