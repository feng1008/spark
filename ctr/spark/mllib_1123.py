#!/usr/bin/python
#-*- coding:utf8 -*-

def extract_feature(filename):
    category_feature=[]
    word_feature=[]
    label=[]

    for lines in open(filename, 'r').xreadlines():
        try:
            spotid, appid, bidid, drt, carrier, city, w_len, albumid, channelid, slotid, w, h, pos, title_info, keyword_set, tag_set, \
            is_click=lines.strip('\r\n').split('\t')
        except:
            continue
        assert(int(is_click)==0 or int(is_click)==1)
        label.append(int(is_click))

        drt=drt.split('+')[1].split(':')[0]
        # category_list=[spotid, appid, drt, carrier, city, albumid, channelid, slotid, w, h, pos, keyword_set, tag_set]
        category_list=['spotid&'+spotid, 'appid&'+appid, 'drt&'+drt, 'carrier&'+carrier, 'city&'+city, 'albumid&'+albumid, \
        'channelid&'+channelid, 'slotid&'+slotid, 'w&'+w, 'h&'+h, 'pos&'+pos, 'keyword_set&'+keyword_set, 'tag_set&'+tag_set]
        category_feature.append(category_list)
    return category_feature, label

def generate_file(feature, label, output_file):
    feature_dict={}
    feature_num=1

    f=open(output_file, 'w')

    for lst, y in zip(feature, label):
        temp_str=''
        temp_set=set()
        for ele in lst:
            for t in ele.split('&')[-1].split('|'):
                a_ele=ele.split('&')[0]+'&'+t
                if a_ele not in feature_dict:
                    feature_dict[a_ele]=feature_num
                    temp_set.add(feature_num)
                    feature_num+=1
                else:
                    temp_set.add(feature_dict[a_ele])

        temp_list=sorted(temp_set)
        for ele in temp_list:
            temp_str+=str(ele)+':1'+' '
        f.write(str(y)+' '+temp_str[:-1]+'\n')
    f.close()

if __name__=="__main__":
    feature, label=extract_feature('data10w')

    generate_file(feature, label, 'test_file_10w')