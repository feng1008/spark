#!/usr/bin/python
#-*- coding:utf8 -*-
import operator

topk_dict={}
topk_set=set()

def get_topk_set(filename, k):
    global topk_set

    count=0
    for lines in open(filename, 'r').xreadlines():
        if count==k:
            break
        count+=1
        topk_set.add(lines.strip('\r\n').split(':')[0])

def get_topk_dist(input_file, output_file):
    global topk_dict
    global topk_set

    f1=open(input_file, 'r')
    f2=open(output_file, 'w')
    for lines in f1.xreadlines():
        imei, package_str=lines.strip('\r\n').split('\t')
        for app in package_str.split('|'):
            if app in topk_set:
                if topk_dict.has_key(app):
                    topk_dict[app]+=1
                else:
                    topk_dict[app]=1

    f1.close()
    topk_list=sorted(topk_dict.items(), key=operator.itemgetter(1), reverse=True)
    for x in topk_list:
        f2.write(x[0]+'\t'+str(x[1])+'\n')
    f2.close()

if __name__=='__main__':
    get_topk_set('app_num', 100)
    get_topk_dist('user_install_list', 'topk_dist')