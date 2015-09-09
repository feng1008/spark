#!/usr/bin/python
#-*- coding:utf8 -*-
import codecs
import xlrd

def gen_feature(input_file, output_file):
    feature_set=set(("device_id", "os", "yob", "gender"))

    f1=open(input_file, 'r')
    f2=open(output_file, 'w')
    for lines in f1.xreadlines():
        blank_str, json_str=lines.strip('\r\n').split(' > > > ')
        for ele in blank_str.split(' '):
            if ele.split(':')[0] not in feature_set:
                continue
            if ele.startswith("device_id"):
                device_id=ele.split(':')[-1].strip('"')
            elif ele.startswith("os"):
                os=ele.split(':')[-1].strip('"')
            elif ele.startswith("yob"):
                yob=ele.split(':')[-1].strip('"')
            elif ele.startswith("gender"):
                gender=ele.split(':')[-1].strip('"')
            else:
                continue
        result=device_id+'\t'+os+'\t'+gender+'\t'+yob
        f2.write(result+'\n')
    f1.close()
    f2.close()

# def extract_feature(input_file, output_file):
#     f1=codecs.open(input_file, 'r', 'utf-8')
#     f2=codecs.open(output_file, 'w', 'utf-8')
#     count=0
#     for lines in f1.readlines():
#         if count==0:
#             count+=1
#             continue
#         userid, sex, birthday, married, Occupation, education, imei, mac=lines.strip('\r\n').split(',')
#         if imei=='':
#             continue
#         sex=str(int(sex)-1)
#         f2.write(imei+'\t'+sex+'\n')
#     f1.close()
#     f2.close()

def extract_feature(input_file, output_file):
    f=codecs.open(output_file, 'w')
    excel = xlrd.open_workbook('user_info.xls')
    sheet = excel.sheets()[0]
    for row in range(1,sheet.nrows):
        imei=sheet.row_values(row)[6]
        sex=sheet.row_values(row)[1]
        if imei=='' or int(sex)<1 or imei=='unknown':
            continue
        sex=str(int(sex)-1)
        f.write(imei+'\t'+sex+'\n')
    f.close()

def split_train_test(input_file, train, test):
    f=codecs.open(input_file, 'r', 'utf-8')
    f_train=codecs.open(train, 'w', 'utf-8')
    f_test=codecs.open(test, 'w', 'utf-8')
    male_count=0
    female_count=0
    for lines in f.readlines():
        try:
            imei, sex, packages=lines.strip('\r\n').split('\t')
            if sex=='0':
                if male_count%2==0:
                    f_train.write(sex+'\t'+packages+'\n')
                else:
                    f_test.write(sex+'\t'+packages+'\n')
                male_count+=1
            else:
                if female_count%2==0:
                    f_train.write(sex+'\t'+packages+'\n')
                else:
                    f_test.write(sex+'\t'+packages+'\n')
                female_count+=1
        except:
            continue
    f.close()
    f_train.close()
    f_test.close()

def resample(src_tr, src_te, des_tr, des_te):
    f_src_tr=codecs.open(src_tr, 'r', 'utf-8')
    f_src_te=codecs.open(src_te, 'r', 'utf-8')
    f_des_tr=codecs.open(des_tr, 'w', 'utf-8')
    f_des_te=codecs.open(des_te, 'w', 'utf-8')
    tr_count=0
    te_count=0
    for lines in f_src_tr.readlines():
        sex, packages=lines.strip('\r\n').split('\t')
        if sex!='0' and sex!='1':
            continue
        if sex=='0':
            if tr_count<307:
                f_des_tr.write(lines)
                tr_count+=1
        else:
            f_des_tr.write(lines)

    for lines in f_src_te.readlines():
        sex, packages=lines.strip('\r\n').split('\t')
        if sex!='0' and sex!='1':
            continue
        if sex=='0':
            if te_count<311:
                f_des_te.write(lines)
                te_count+=1
        else:
            f_des_te.write(lines)
    f_src_tr.close()
    f_src_te.close()
    f_des_tr.close()
    f_des_te.close()

def retest(input_file):
    f=open(input_file, 'r')
    result=[0]*4
    for lines in f.readlines():
        try:
            t1,t2=lines.strip('\r\n').split('->')
        except:
            continue
        t1=float(t1)
        t2=float(t2)
        if t1<0.5 and t2<0.5:
            result[0]+=1
        elif t1<0.5 and t2>0.5:
            result[1]+=1
        elif t1>0.5 and t2<0.5:
            result[2]+=1
        else:
            result[3]+=1

    print result
        
if __name__=='__main__':
    # gen_feature('model/text', 'user_feature')
    # extract_feature('user_info.xls', 'user_sex')
    # split_train_test('data/user_sex_packages', 'data/train', 'data/test')
    # resample('data/train2', 'data/test2','data/train', 'data/test')
    retest('two')