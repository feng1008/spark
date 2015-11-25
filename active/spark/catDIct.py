#!/usr/bin/python
#-*- coding:utf8 -*-
import codecs
import sys

def purfy(input_file, output_file):
    '''
        purfy('label_aos_unpurify.txt', 'label_aos.txt')
    '''
    f1=codecs.open(input_file, 'r', 'utf-8')
    f2=codecs.open(output_file, 'w', 'utf-8')
    for lines in f1.readlines():
        # print lines
        # print len(lines.strip('\r\n').split('\t'))
        try:
            app_name, platform, name1, name2, x, first_class, second_class ,count=lines.strip('\r\n').split('\t')
        except:
            continue
        if first_class!=u"购物电商" or len(second_class.split(':'))<2:
            continue
        f2.write(app_name+'\t'+platform+'\t'+first_class+'\t'+second_class.split(':')[1]+'\n')
    f1.close()
    f2.close()

def catSecond(input_file, output_file):
    f1=codecs.open(input_file, 'r', 'utf-8')
    f2=codecs.open(output_file, 'w', 'utf-8')

    app_dict={}
    f_class=''
    for lines in f1.readlines():
        try:
            app_name, platform, first_class, second_class=lines.strip('\r\n').split('\t')
        except:
            continue
        f_class=first_class
        if app_dict.has_key(app_name+'\t'+platform):
            app_dict[app_name+'\t'+platform]+='|'+second_class
        else:
            app_dict[app_name+'\t'+platform]=second_class

    for key,value in app_dict.items():
        f2.write(key+'\t'+f_class+'\t'+value+'\n')

def catDict(aos_input, ios_input, dict_output):
    f1=codecs.open(aos_input, 'r','utf-8')
    f2=codecs.open(ios_input, 'r','utf-8')
    f3=codecs.open(dict_output, 'w', 'utf-8')
    for lines in f1.readlines():
        try:
            name, platform, first_class, second_class=lines.strip('\r\n').split('\t')
        except:
            continue
        f3.write(name+'\t'+'android'+'\t'+first_class+'\t'+second_class+'\n')
    f1.close()

    for lines in f2.readlines():
        try:
            name, first_class, second_class=lines.strip('\r\n').split('\t')
        except:
            continue
        f3.write(name+'\t'+'ios'+'\t'+first_class+'\t'+second_class+'\n')
    f2.close()
    f3.close()

if __name__=='__main__':
    # catSecond(sys.argv[1], sys.argv[2])
    catDict(sys.argv[1], sys.argv[2], sys.argv[3])