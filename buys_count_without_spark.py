#!/usr/bin/python
#coding="utf-8"
import sys;
from datetime import datetime;
import codecs;

#read useful data from file
def readData(inputFile):
    f=codecs.open(inputFile,'r','utf-8');
    result=[];
    # import pdb;pdb.set_trace();
    for lines in f.readlines():
        try:
            idClass, userId, itemName, item_num=lines.split(',');

            if userId=="" or idClass=="" or itemName=="" or item_num=="":
                continue;
            result.append((itemName, userId+'\t'+idClass+'\t'+item_num));
        except:
            continue;
    return result;

def genItemDict(data):
    item_sum_dict={};
    user_item_dict={}
    # import pdb;pdb.set_trace();
    for lines in data:
        itemName=lines[0];
        userId, idClass, item_num=lines[1].split('\t');
        if item_sum_dict.has_key(itemName):
            item_sum_dict[itemName]+=int(item_num.strip('\n'));
        else:
            item_sum_dict[itemName]=int(item_num.strip('\n'));

        user_id_item=userId+'\t'+idClass+'\t'+itemName;
        if user_item_dict.has_key(user_id_item):
            user_item_dict[user_id_item]+=int(item_num.strip('\n'));
        else:
            user_item_dict[user_id_item]=int(item_num.strip('\n'));

    return item_sum_dict, user_item_dict;

def reduceDict(item_sum_dict, user_item_dict):
    result={};
    for key, value in user_item_dict.items():
        userId, idClass, itemName=key.split('\t');
        # import pdb;pdb.set_trace();
        try:
            item_sum=item_sum_dict[itemName];
        except:
            continue;
        if result.has_key(userId+'\t'+idClass):
            result[userId+'\t'+idClass].append((itemName, int(value), 100*float(value)/float(item_sum)));
        else:
            result[userId+'\t'+idClass]=[(itemName, int(value), 100*float(value)/float(item_sum))];
    
    for keys, values in result.items():
        temp_value=sorted(values, key=lambda x:x[2], reverse=True);
        str_value="";
        for lines in temp_value:
            ratio=round(float(lines[2]), 8);
            str_value+='\t'+lines[0]+':'+str(lines[1])+':'+str(lines[2]);
        result[keys]=str_value;
    return result;

def saveResult(dict, outputFile):
    f=codecs.open(outputFile,'w','utf-8');
    for key, value in dict.items():
        f.write(key+'\t'+value+'\n');
    f.close();

if __name__=="__main__":
    inputFile=sys.argv[1];
    outputFile=sys.argv[2];

    data=readData(inputFile);
    item_sum_dict, user_item_dict=genItemDict(data);
    reduce_dict=reduceDict(item_sum_dict, user_item_dict);
    saveResult(reduce_dict, outputFile);
