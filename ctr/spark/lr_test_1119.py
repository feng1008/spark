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

    epsilon=1e-6
    for i in range(len(act)):
        if act[i]==0.0:
            act[i]=epsilon
        if pred[i]==0.0:
            pred[i]=epsilon
    # act=[epsilon if x==0.0 for x in act]
    # pred=[epsilon if x==0.0 for x in pred]
    error=sum([abs((act[i]-pred[i])/act[i]) for i in range(len(act))])/len(act)
    print error
    return error

def extract_feature(filename):
    category_feature=[]
    num_feature=[]
    y=[]

    for lines in open(filename, 'r').xreadlines():
        try:
            spotid, appid, bidid, drt, carrier, city, w_len, albumid, channelid, slotid, w, h, pos, title_info, keyword_set, tag_set, \
            is_click=lines.strip('\r\n').split('\t')
        except:
            continue

        y.append(int(is_click))
        num_feature.append(int(float(w_len)/1000))

        drt=drt.split('+')[1].split(':')[0]
        # category_list=[spotid, appid, drt, carrier, city, albumid, channelid, slotid, w, h, pos, keyword_set, tag_set]
        category_list=[spotid, appid, drt, carrier, city, albumid, channelid, slotid, w, h, pos]
        category_feature.append(category_list)
    return category_feature, num_feature, y

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

    cate_feat_array=cate_feat.toarray()

    if len(cate_feat_array)!=len(num_feat):
        print "The length of category feature and number feature are not the same!"
        return
    for a, b in zip(cate_feat_array, num_feat):
        # import pdb;pdb.set_trace()
        result.append(a+b)
    return coo_matrix(result, dtype=np.int8)

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

if __name__=="__main__":
    train_cate_feat, train_num_feat, train_y=extract_feature('train_1119')
    test_cate_feat, test_num_feat, test_y=extract_feature('test_1119')
    # train_cate_feat, train_num_feat, train_y=extract_feature('train_data1119')
    # test_cate_feat, test_num_feat, test_y=extract_feature('test_data1119')
    train_cate_x, test_cate_x=vectorlize(train_cate_feat, test_cate_feat)
    train_x=cat_feature(train_cate_x, train_num_feat)
    test_x=cat_feature(test_cate_x, test_num_feat)

    # lr_test(train_x, train_y, test_x, test_y)
    GBDT_test(train_x, train_y, test_x, test_y)