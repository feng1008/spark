#!/usr/bin/python
#-*- coding:utf8 -*-
import codecs
import xlrd

imei_dict={}

def gen_feature(input_file, output_file, dup_file):
    global imei_dict
    multi_set=set()

    feature_set=set(("device_id", "os", "yob", "gender"))

    f1=open(input_file, 'r')
    f2=open(output_file, 'w')
    f3=open(dup_file, 'w')
    for lines in f1.xreadlines():
        try:
            blank_str, json_str=lines.strip('\r\n').split(' > > > ')
        except:
            continue
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
        if device_id=='' or os!='android' or gender not in ['MALE', 'FEMALE'] or device_id in multi_set:
            continue
        try:
            age=int(yob)
        except:
            continue
        if age<10 or age>80:
            continue
        if device_id not in imei_dict:
            imei_dict[device_id]=os+'\t'+yob+'\t'+gender
        else:
            if imei_dict[device_id]==os+'\t'+yob+'\t'+gender:
                continue
            else:
                del imei_dict[device_id]
                multi_set.add(device_id)
                # f3.write(device_id+'\t'+os+'\t'+gender+'\t'+yob)
                continue
        result=device_id+'\t'+os+'\t'+gender+'\t'+yob
        # f2.write(result+'\n')
    f1.close()
    for k, v in imei_dict.items():
        f2.write(k+'\t'+v+'\n')
    f2.close()
    f3.close()

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

def feature_extract(input_file, aos_output_file, ios_output_file):
    aos_f=open(aos_output_file, 'w')
    ios_f=open(ios_output_file, 'w')
    with open(input_file, 'r') as f:
        for lines in f.xreadlines():
            device_id, os, gender, yob=lines.strip('\r\n').split('\t')
            if device_id=='' or gender=='' or gender=='UNKNOWN':
                continue
            if gender=='MALE':
                gen='0'
            elif gender=='FEMALE':
                gen='1'
            else:
                continue
            if os=='android':
                aos_f.write(device_id+'\t'+os+'\t'+gen+'\t'+yob+'\n')
            elif os=='ios':
                ios_f.write(device_id+'\t'+os+'\t'+gen+'\t'+yob+'\n')
            else:
                continue
    aos_f.close()
    ios_f.close()

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
            imei, sex, age, packages=lines.strip('\r\n').split('\t')
            if sex=='0':
                if male_count>=1886:
                    continue
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

def split_female(input_file, aos_female, ios_female):
    f_aos=open(aos_female, 'w')
    f_ios=open(ios_female, 'w')
    for lines in open(input_file, 'r').xreadlines():
        user_id, os=lines.strip('\r\n').split('\t')
        if os=='imei':
            f_aos.write(user_id+'\t'+'1'+'\n')
        elif os=='idfa':
            f_ios.write(user_id+'\t'+'1'+'\n')
        else:
            continue
    f_aos.close()
    f_ios.close()

def split_gender(input_file, male_output, female_output):
    f1=open(male_output, 'w')
    f2=open(female_output, 'w')
    for lines in open(input_file, 'r').xreadlines():
        # imei, gender, age, packages=lines.strip('\r\n').split('\t')
        gender, packages=lines.strip('\r\n').split('\t')
        if gender=='MALE' or gender=='0':
            f1.write('0'+'\t'+packages+'\n')
        elif gender=='FEMALE' or gender=='1':
            f2.write('1'+'\t'+packages+'\n')
        else:
            continue
    f1.close()
    f2.close()

def cat_feature(gender_dict_file, aos_last5, aos_data):
    gender_dict={}

    for lines in open(gender_dict_file, 'r').xreadlines():
        imei, os, age, gender=lines.strip('\r\n').split('\t')
        if gender=='FEMALE':
            gender='1'
        elif gender=='MALE':
            gender='0'
        else:
            continue
        gender_dict[imei]=gender
        # if imei not in gender_dict:
        #     gender_dict[imei]=gender
#    import pdb;pdb.set_trace()
    f=open(aos_data, 'w')
    for lines in open(aos_last5, 'r').xreadlines():
        imei, packages=lines.strip('\r\n').split('\t')
        if imei not in gender_dict or len(packages.split('|'))<10:
            continue
        f.write(gender_dict[imei]+'\t'+packages+'\n')
    f.close()

def gen_install_feature(input_file, output_file):
    f=open(output_file, 'w')
    for lines in open(input_file, 'r').xreadlines():
        imei, gender, age, packstr=lines.strip('\r\n').split('\t')
        if len(packstr.split('|'))<10:
            continue
        if gender=='FEMALE':
            gender='1'
        elif gender=='MALE':
            gender='0'
        else:
            continue
        f.write(gender+'\t'+packstr+'\n')
    f.close()
        
if __name__=='__main__':
    # gen_feature('toutiao_bidrequest', 'user_feature', 'dup_imei')
    # feature_extract('user_feature', 'aos_user_sex', 'ios_user_sex')
    # extract_feature('user_info.xls', 'user_sex')
    # split_train_test('data/aos_users', 'data/aos_train', 'data/aos_test')
    # resample('data/train2', 'data/test2','data/train', 'data/test')
    # retest('two')

    # split_female('data/female_data', 'data/aos_female', 'data/ios_female')
    # split_gender('data/aos_last5_feature', 'data/male_last5', 'data/female_last5')
    # cat_feature('user_feature', 'data/aos_last5_feature', 'data/aos_last5_data')
    # gen_install_feature('data/aos_install_feature', 'data/aos_install_data')
    split_gender('data/aos_last5_data', 'data/male_last5', 'data/female_last5')
    split_gender('data/aos_install_data', 'data/male_install', 'data/female_install')