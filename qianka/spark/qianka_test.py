#!/usr/bin/python
#-*- coding:utf8 -*-
import codecs, csv
import pickle
import os
from sklearn.feature_extraction.text import CountVectorizer
from math import log

general_app=set()
app_vocabulary=set()
app_pay_dict={}

def gen_vocab(filename):
    global app_vocabulary

    for lines in open(filename, 'r').xreadlines():
        app_vocabulary.add(lines.strip('\r\n').replace('.', '_'))
    pickle.dump(app_vocabulary, open('data/app_vocab.pkl','w'))

def read_general_app(filename):
    global general_app

    for lines in open(filename, 'r').readlines():
        app_name=lines.strip('\r\n').split('\t')[0]
        general_app.add(app_name)
    pickle.dump(general_app, open('data/gen_app.pkl','w'))

def get_2fee_app_dict(filename):
    global app_pay_dict

    csvfile=file(filename, 'rb')
    reader=csv.reader(csvfile)
    for lines in reader:
        app_name=lines[2]
        app_price=float(lines[3])
        if not app_pay_dict.has_key(app_name):
            app_pay_dict[app_name]=app_price
        else:
            continue
    pickle.dump(app_pay_dict, open('data/app_pay_dict.pkl', 'w'))

def gen_add_feature(train_x):
    global general_app
    global app_pay_dict

    result=[]

    for lines in train_x:
        fee_c=0
        fee_sum=0.0
        gen_sum=0
        apps=lines.strip('\r\n').split(' ')
        app_num=len(apps)
        if app_num<5:
            result.append([0.0, 0, 0.0])
            continue
        for app in apps:
            if app in general_app:
                gen_sum+=1
            if app in app_pay_dict:
                fee_c+=1
                fee_sum+=app_pay_dict[app]
        gen_ratio='%0.3f'%(float(gen_sum)/app_num)
        if fee_c!=0:
            fee_avg='%0.3f'%(log(float(fee_sum)/fee_c))
            fee_c=log(fee_c)
        else:
            fee_avg=str(0.0)
        result.append([float(gen_ratio), float(fee_c), float(fee_avg)]) 
    return result

def vectorize_data(test_data):
    global app_vocabulary

    vectorize=CountVectorizer(vocabulary=list(app_vocabulary))
    # import pdb;pdb.set_trace()
    counts_train=vectorize.fit_transform(test_data)

    return counts_train

def lr_l2_test(train_x):
    if not os.path.exists('model/lr_l2_clf.pkl'):
        print "model lr_l2_clf.pkl does not exist!"
        return 
    else:
        output=open('model/lr_l2_clf.pkl', 'rb')
        clf=pickle.load(output)

    y=clf.predict_proba(train_x)    
    y=[float('%0.3f'%x[-1]) for x in y]
    y2=[1  if x>0.5 else 0 for x in y ]
    # print y2
    return y

def GBDT_test(test_x):
    # print "gradient boosting classifier:"  
    if not os.path.exists('model/GBDT.pkl'):
        print "model GBDT.pkl does not exist!"
        return 
    else:
        output=open('model/GBDT.pkl', 'rb')
        est=pickle.load(output)

    y=list(est.predict(test_x))
    # import pdb;pdb.set_trace()
    # error=sum([1 if label[i]!=y[i] else 0 for i in range(len(y))])
    # print error/float(len(label))
    return y

def cat_feature(x1, x2):
    result=[]
    if len(x1)!=len(x2):
        print "The added feature and predict label are not the same length."
        return result
    for a, b in zip(x1, x2):
        result.append(a+[b])
    return result

def read_app_str(input_file):
    result=[]
    for lines in open(input_file).xreadlines():
        try:
            idfa, app_list=lines.strip('\r\n').split('\t')
        except:
            continue
        apps=' '.join([x.replace('.', '_') for x in app_list.split('|')])
        result.append(apps)
    return result

def main():
    global general_app
    global app_vocabulary
    global app_pay_dict

    # general_app=pickle.load(open('data/gen_app.pkl', 'r'))
    # app_vocabulary=pickle.load(open('data/app_vocab.pkl', 'r'))
    # app_pay_dict=pickle.load(open('data/app_pay_dict.pkl', 'r'))
    gen_vocab('data/ios_top3w')
    read_general_app('data/general_app')
    get_2fee_app_dict('data/2.3wfee_package.csv')
    
    # import pdb;pdb.set_trace()
    app_str=read_app_str('test')
    test_x=vectorize_data(app_str)

    y=lr_l2_test(test_x)
    
    add_feat=gen_add_feature(app_str)
    retest_feat=cat_feature(add_feat, y)
    label=GBDT_test(retest_feat)

if __name__=='__main__':
    main()
    