#!/usr/bin/python
#coding="utf-8"
import codecs;
from datetime import datetime, timedelta;
import sys;

DAYS=[30, 60, 90];

#read useful data from file
def readData(filename):
    f=codecs.open(filename,'r','utf-8');
    now=datetime.now();
    data=[];
    for lines in f.readlines():
        t_lines=lines.split(',');
        id_class=t_lines[0];
        user_id=t_lines[1];
        item_name=t_lines[2];
        item_num=t_lines[3];
        item_time=t_lines[4].strip('\n');
        # import pdb;pdb.set_trace();
        strp_time=datetime.strptime(item_time, '%Y-%m-%d %H:%M:%S');
        dur_time=(now-strp_time).days;

        if id_class=="" or user_id=="" or item_name=="" or item_num=="":
            continue;

        if dur_time<DAYS[0]:
            date_dur_type=DAYS[0];
        elif dur_time<DAYS[1]:
            date_dur_type=DAYS[1];
        elif dur_time<DAYS[2]:
            date_dur_type=DAYS[2];
        else:
            continue;
        data.append([id_class, user_id, item_name, item_num, date_dur_type]);
    f.close();
    return data;

def generateDict(data):
    dict0={};
    dict1={};
    dict2={};

    for lines in data:
        id_class, user_id, item_name, item_num, date_dur_type=lines;
        item_key=user_id+'\t'+id_class;
        item_value=item_name+'\t'+item_num;

        if date_dur_type==DAYS[0]:
            if dict0.has_key(item_key):
                dict0[item_key]+='|'+item_value;
                dict1[item_key]+='|'+item_value;
                dict2[item_key]+='|'+item_value;
            else:
                dict0[item_key]=item_value;
                dict1[item_key]=item_value;
                dict2[item_key]=item_value;
        elif date_dur_type==DAYS[1]:
            if dict1.has_key(item_key):
                dict1[item_key]+='|'+item_value;
                dict2[item_key]+='|'+item_value;
            else:
                dict1[item_key]=item_value;
                dict2[item_key]=item_value;
        elif date_dur_type==DAYS[2]:
            if dict2.has_key(item_key):
                dict2[item_key]+='|'+item_value;
            else:
                dict2[item_key]=item_value;
        else:
            continue;
    return dict0 ,dict1, dict2;

def dictReduce(dicts,days):
    dicts_ret={};

    for key, value in dicts.items():
        user_id_class=key;
        goods_items=value.split('|');
        user_item_dict={};
        # import pdb;pdb.set_trace();
        for g_itms in goods_items:
            item_name=g_itms.split('\t')[0];
            item_num=g_itms.split('\t')[1];
            if user_item_dict.has_key(item_name):
                user_item_dict[item_name]+=int(item_num);
            else:
                user_item_dict[item_name]=int(item_num);

        dict_str=str(days)+' days:';
        for it_key,it_values in user_item_dict.items():
            dict_str+=' '+it_key+':'+str(it_values);

        dicts_ret[key]=dict_str;
    return dicts_ret;

def catDicts(dict_list):
    red_dict0=dict_list[0];
    red_dict1=dict_list[1];
    red_dict2=dict_list[2];

    results={};
    # import pdb;pdb.set_trace();
    for key,value in red_dict0.items():
        item_value=value;
        if red_dict1.has_key(key):
            item_value+='\t'+red_dict1[key];
            del red_dict1[key];
        if red_dict2.has_key(key):
            item_value+='\t'+red_dict2[key];
            del red_dict2[key];
        results[key]=item_value;

    for key,value in red_dict1.items():
        item_value=value;
        if red_dict2.has_key(key):
            item_value+='\t'+red_dict2[key];
            del red_dict2[key];
        results[key]=item_value;

    for key,value in red_dict2.items():
        results[key]=value;

    return results;

def saveResult(results, outputFile):
    f=codecs.open(outputFile, 'w', 'utf-8');

    for key, value in results.items():
        f.write(key+'\t'+value+'\n');
    f.close();

def  itemCount(data):
    dict0, dict1, dict2=generateDict(data);

    red_dict0=dictReduce(dict0,days=30);
    red_dict1=dictReduce(dict1,days=60);
    red_dict2=dictReduce(dict2,days=90);

    results=catDicts([red_dict0, red_dict1, red_dict2]);
    return results;

if __name__=="__main__":
    inputFile=sys.argv[1];
    outputFile=sys.argv[2];
    data=readData(inputFile);
    results=itemCount(data);
    saveResult(results, outputFile);
