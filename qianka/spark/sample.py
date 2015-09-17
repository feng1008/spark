#!/usr/bin/python
#-*- coding:utf8 -*-
import sys
import codecs
from string import translate
from string import punctuation

pack_num_dict=None
idfa_set=set()
app_set=set()

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

def sample_idfa(inputFile, outputFile):
    global idfa_set
    user_package_dict={}

    f1=codecs.open(inputFile, 'r', 'utf-8')
    f2=codecs.open(outputFile, 'w', 'utf-8')
    count=1
    for lines in f1.readlines():
        try:
            key_str, ei_str, mac_str, rate_str, cid_str, ifa_str, udid_str, nid_str, le_str, pkgs_str=lines.strip('\r\n').split('\t')

            if ifa_str=='' or ifa_str in idfa_set or is_IDFA(ifa_str)=='':
                continue
            temp_dict=eval(pkgs_str)
            result=ifa_str+'\t'

            if len(temp_dict)<10:
                continue

            for t in temp_dict.keys():
                if t.translate(None, punctuation).lower().isalnum():
                    result+=t+'|'

            if result.split('\t')[1]!='':
                f2.write(result[:-1]+'\n')
                count+=1
            if count>148224:
                break
            else:
                continue
        except:
            continue
    f1.close()
    f2.close()

def read_idfa_set(filename):
    global idfa_set

    f=codecs.open(filename, 'r', 'utf-8')
    for lines in f.readlines():
        idfa_set.add(lines.strip('\r\n').replace('-','').lower())
    f.close()

def generate_whole_dict(postive_file, negative_file, app_set_file):
    app_set=set()
    f1=codecs.open(postive_file, 'r', 'utf-8')
    f2=codecs.open(negative_file, 'r', 'utf-8')
    f3=codecs.open(app_set_file, 'w', 'utf-8')
    for lines in f1.readlines():
        idfa, package_str=lines.strip('\r\n').split('\t')
        temp_set=set(package_str.split('|'))
        app_set |=temp_set

    for lines in f2.readlines():
        idfa, package_str=lines.strip('\r\n').split('\t')
        temp_set=set(package_str.split('|'))
        app_set |=temp_set

    f1.close()
    f2.close()

    app_list=list(app_set)
    for app in app_list:
        f3.write(app+'\n')
    f3.close()

def load_app_set(filename):
    global app_set

    f=codecs.open(filename, 'r', 'utf8')
    for lines in f.readlines():
        app_set.add(lines.strip('\r\n'))
    f.close()

def generateVector(postive_file, negative_file, output_file):
    global app_set

    app_list=list(app_set)
    app_len=len(app_list)
    f1=codecs.open(postive_file, 'r', 'utf-8')
    f2=codecs.open(negative_file, 'r', 'utf-8')
    f3=codecs.open(output_file, 'w', 'utf-8')
    for lines in f1.readlines():
        one_list=[0]*(app_len+1)
        one_list[0]=1
        app_name, package_str=lines.strip('\r\n').split('\t')
        for app in package_str.split('\t'):
            try:
                one_list[app_list.index(app)]=1
            except:
                continue
        result=str(one_list)[1:-1].replace(',',' ')
        f3.write(result+'\n')

    for lines in f2.readlines():
        one_list=[0]*(app_len+1)
        app_name, package_str=lines.strip('\r\n').split('\t')
        for app in package_str.split('\t'):
            try:
                one_list[app_list.index(app)]=1
            except:
                continue
        result=str(one_list)[1:-1].replace(',',' ')
        f3.write(result+'\n')
    f1.close()
    f2.close()
    f3.close()

if __name__=='__main__':
    read_idfa_set('idfa/idfa')
    sample_idfa('package_install/packages', 'data_0831/negative')
    # sample_idfa('package_install/temp', 'negative_sample')
    # generate_whole_dict('LR/positive_sample','LR/negative_sample', 'LR/app_set')
    # load_app_set('LR/app_set')
    # generateVector('LR/positive_sample', 'LR/negative_sample', 'train_data')