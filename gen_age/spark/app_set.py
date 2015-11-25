#!/usr/bin/python
#-*- coding:utf8 -*-
import codecs

def get_appset(input_file, output_file):
    f1=codecs.open(input_file, 'r', 'utf-8')
    f2=codecs.open(output_file, 'w', 'utf-8')
    for lines in f1.xreadlines():
        t=lines.strip('\r\n').split('\t')
        app_name=t[0]
        f2.write(app_name+'\n')
    f1.close()
    f2.close()

if __name__=='__main__':
    get_appset('aos_top3w_temp', 'aos_top3w')
    get_appset('ios_top3w_temp', 'ios_top3w')