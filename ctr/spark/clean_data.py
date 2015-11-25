#!/usr/bin/python
#-*- coding:utf8 -*-

def fill_blank(input_file, output_file):
    '''
        fill_blank('all_data', 'data')
    '''
    f=open(output_file, 'w')
    for lines in open(input_file, 'r').xreadlines():
        t=lines.strip('\r\n').split('\t')
        if len(t)!=8:
            continue
        if t[3]=='':
            t[3]='F'
        f.write('\t'.join(t)+'\n')
    f.close()

def split_train_test(input_file, train_file, test_file):
    f1=open(train_file, 'w')
    f2=open(test_file, 'w')
    count=1
    for lines in open(input_file, 'r').xreadlines():
        # spotid, appid, bidid, drt, carrier, city, w_len, albumid, channelid, slotid, w, h, pos, title_info, keyword_set, tag_set, \
        # is_click=lines.strip('\r\n').split('\t')
        if count%2==0:
            f=f1
        else:
            f=f2
        f.write(lines)
        count+=1
    f1.close()
    f2.close()


if __name__=='__main__':
    # fill_blank('all_data', 'data')
    split_train_test('data1119', 'train_data1119', 'test_data1119')