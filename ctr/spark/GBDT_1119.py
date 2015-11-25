#!/usr/bin/python
#-*- coding:utf8 -*-
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn import linear_model
import scipy as sp
from scipy.sparse import coo_matrix
from matplotlib import pyplot as plt
import os, pickle

def llfun(act, pred):
    epsilon = 1e-15
    pred = sp.maximum(epsilon, pred)
    pred = sp.minimum(1-epsilon, pred)
    ll = sum(act*sp.log(pred) + sp.subtract(1,act)*sp.log(sp.subtract(1,pred)))
    # ll = sum(ll)
    ll = ll * -1.0/len(act)

    print ll
    return ll

def mse(act, pred):
    error=sum([(act[i]-pred[i])**2 for i in range(len(act))])/len(act)
    print error
    return error

def mae(act, pred):
    # act=[epsilon if x==0.0 for x in act]
    # pred=[epsilon if x==0.0 for x in pred]
    error=sum([abs((act[i]-pred[i])) for i in range(len(act))])/len(act)
    print error
    return error

def extract_feature(filename):
    category_feature=[]
    word_feature=[]
    y=[]

    for lines in open(filename, 'r').xreadlines():
        try:
            spotid, appid, bidid, drt, carrier, city, w_len, albumid, channelid, slotid, w, h, pos, title_info, keyword_set, tag_set, \
            is_click=lines.strip('\r\n').split('\t')
        except:
            continue
        assert(int(is_click)==0 or int(is_click)==1)
        y.append(int(is_click))

        drt=drt.split('+')[1].split(':')[0]
        # category_list=[spotid, appid, drt, carrier, city, albumid, channelid, slotid, w, h, pos, keyword_set, tag_set]
        category_list=[spotid, appid, drt, carrier, city, albumid, channelid, slotid, w, h, pos]
        category_feature.append(category_list)
        word_feature.append([keyword_set, tag_set])
    return category_feature, word_feature, y

def vectorlize(train_x, test_x):
    input_x=train_x+test_x
    x=input_x

    rows, cols=np.shape(input_x)
    feature=[]
    for j in range(cols):
        feature_dict={}
        feature_num=1
        for i in range(rows):
            for ele in input_x[i][j].split('|'):
                if ele not in feature_dict:
                    feature_dict[ele]=feature_num
                    feature_num+=1
        feature.append(feature_dict)

    # import pdb;pdb.set_trace()
    train_len=len(train_x)
    test_len=len(test_x)

    feature_length={}
    for i in range(cols):
        feature_length[i]=max(feature[i].values())

    tr_x=[]
    te_x=[]
    
    for i in range(train_len):
        temp_x=[]
        for j in range(cols):
            temp_fea=[0]*feature_length[j]
            for ele in input_x[i][j].split('|'):
                temp_fea[feature[j][ele]-1]=1
            temp_x+=temp_fea
        tr_x.append(temp_x)

    for i in range(test_len):
        temp_x=[]
        for j in range(cols):
            temp_fea=[0]*feature_length[j]
            for ele in input_x[i][j].split('|'):
                temp_fea[feature[j][ele]-1]=1
            temp_x+=temp_fea
        te_x.append(temp_x)
  
    # enc=OneHotEncoder()
    # enc.fit(tr_x+te_x)

    # return enc.transform(tr_x), enc.transform(te_x)

    sparse_train_x=coo_matrix(tr_x, dtype=np.int8)
    sparse_test_x=coo_matrix(te_x, dtype=np.int8)
    return sparse_train_x, sparse_test_x

def cat_feature(cate_feat, num_feat):
    result=[]

    if len(cate_feat)!=len(num_feat):
        print "The length of category feature and number feature are not the same!"
        return
    for a, b in zip(cate_feat, num_feat):
        result.append(a+b)
    return result

def write_result(ty, y, output_file):
    ty=[100*x for x in ty]
    y=[100*x for x in y]
    f=open(output_file, 'w')
    for m, n in zip(ty, y):
        n=float('%.4f'%n)
        f.write(str(m)+'\t'+str(n)+'\n')
    f.close()

def lr_test(train_x, train_y, test_x, test_y):
    clf=linear_model.LogisticRegression(penalty='l2')
    clf.fit(train_x, train_y)

    y1=clf.predict_proba(train_x)
    y2=clf.predict_proba(test_x)

    y1=[x[-1] for x in y1]
    y2=[x[-1] for x in y2]

    write_result(train_y, y1, 'y1')
    write_result(test_y, y2, 'y2')

    llfun(train_y, y1)
    llfun(test_y, y2)

    mae(train_y, y1)
    mae(test_y, y2)

def GBDT_test(train_x, train_y, test_x, test_y):
    print "Gradient Boosting Classifier:" 
    from sklearn.ensemble import GradientBoostingRegressor
    if not os.path.exists('model/GBDT.pkl'):
        clf = GradientBoostingRegressor(n_estimators=30, learning_rate=1.0, max_depth=1, random_state=0)
        clf.fit(train_x, train_y)

        output=open('model/GBDT.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/GBDT.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict(train_x)
    y2=clf.predict(test_x)

    llfun(train_y, y1)
    llfun(test_y, y2)

    mae(train_y, y1)
    mae(test_y, y2)
    return y2

def word_feature_word(word_feature):
    keyword_dict={}
    tag_dict={}

    tag_num=0
    for lines in word_feature:
        for ele in lines[0].split('|'):
            if ele in keyword_dict:
                keyword_dict[ele]+=1
            else:
                keyword_dict[ele]=1

        for ele in lines[1].split('|'):
            if ele not in tag_dict:
                tag_dict[ele]=tag_num
                tag_num+=1

    s_key_list=sorted(keyword_dict.items(), key=lambda x:x[1], reverse=True)[:1000]

    keyword_dict.clear()
    keyword_dict={}

    for x, y in enumerate(s_key_list):
        # import pdb;pdb.set_trace()
        keyword_dict[y[0]]=x

    result=[]

    key_dict_len=len(keyword_dict)
    tag_dict_len=len(tag_dict)
    for lines in word_feature:
        temp=[0]*(key_dict_len+tag_dict_len)
        for ele in lines[0].split('|'):
            if ele in keyword_dict:
                temp[keyword_dict[ele]]=1

        for ele in lines[1].split('|'):
            temp[key_dict_len+tag_dict[ele]]=1
        result.append(temp)
    return result

def cal_mean_ctr(p_y, t_y):
    if len(p_y)!=len(t_y):
        print "the length of predict ctr are not the same!"

    sum_click=0.0
    num_click=0
    sum_unclick=0.0
    num_unclick=0
    for i in range(len(p_y)):
        # import pdb;pdb.set_trace()
        assert(t_y[i]==0 or t_y[i]==1)
        if t_y[i]<1:
            num_unclick+=1
            sum_unclick+=p_y[i]
        else:
            num_click+=1
            sum_click+=p_y[i]
    print sum_click/num_click
    print sum_unclick/num_unclick

if __name__=="__main__":
    train_cate_x, train_word_feature, train_y=extract_feature('train_1119')
    test_cate_x, test_word_feature, test_y=extract_feature('test_1119')

    # train_cate_x, train_word_feature, train_y=extract_feature('train_data1119')
    # test_cate_x, test_word_feature, test_y=extract_feature('test_data1119')

    train_word_x=word_feature_word(train_word_feature)
    test_word_x=word_feature_word(test_word_feature)

    train_x=cat_feature(train_cate_x, train_word_x)
    test_x=cat_feature(test_cate_x, test_word_x)

    # lr_test(train_x, train_y, test_x, test_y)
    y=GBDT_test(train_x, train_y, test_x, test_y)
    cal_mean_ctr(y, test_y)

    # GBDT_test(train_cate_x, train_y, test_cate_x, test_y)