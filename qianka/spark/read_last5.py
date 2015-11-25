#!/usr/bin/python
#-*- coding:utf8 -*-
import codecs
from string import translate
from string import punctuation

def read_file(input_file, output_file):
    f1=codecs.open(input_file, 'r', 'utf-8')
    f2=codecs.open(output_file, 'w', 'utf-8')

    for lines in f1.readlines():
        key_str, ei_str, mac_str, rate_str, cid_str, ifa_str, udid_str, nid_str, le_str, pkgs_str=lines.strip('\r\n').split('\t')

        temp_dict=eval(pkgs_str)
        result=''

        for t in temp_dict.keys():
            if t.translate(None, punctuation).lower().isalnum():
                result+=t+' '

        if result!='':
            f2.write(result[:-1]+'\n')
        else:
            continue

    f1.close()
    f2.close()

if __name__=='__main__':
    read_file('package_install/negative_test', 'LR/negative_test')
    read_file('package_install/negative_train', 'LR/negative_train')