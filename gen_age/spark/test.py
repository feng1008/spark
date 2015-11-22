#!/usr/bin/python
#-*- coding:utf8 -*-
d={}

def purify(input_file, output_file):
    global d

    f=open(output_file, 'w')
    for lines in open(input_file,'r').readlines():
        imei, os, gender, age=lines.strip('\r\n').split('\t')
        if imei not in d:
            d[imei]=os+'\t'+gender+'\t'+age
        else:
            if d[imei]!=os+'\t'+gender+'\t'+age:
                print imei+'\t'+os+'\t'+gender+'\t'+age
    f.close()

if __name__=='__main__':
    purify('user_feature', 'purify_user_feature')
