#!/usr/bin/python
#-*- coding:utf8 -*-
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn import linear_model
import scipy as sp
from matplotlib import pyplot as plt

def llfun(act, pred):
    # import pdb;pdb.set_trace()
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
    error=100*sum([abs((act[i]-pred[i])/act[i]) for i in range(len(act))])/len(act)
    print error
    return error

def read_vec(input_file):
    x=[]
    y=[]

    for lines in open(input_file, 'r').xreadlines():
        t=lines.strip('\r\n').split('\t')
        y.append(float('%.4f'%float(t[-1])))
        x.append(t[:-2])
    return x, y

def vectorlize(train_x, test_x):
    input_x=train_x+test_x
    x=input_x

    rows, cols=np.shape(input_x)

    feature=[]
    # import pdb;pdb.set_trace()
    for j in range(cols):
        feature_dict={}
        feature_num=0
        for i in range(rows):
            if input_x[i][j] not in feature_dict:
                feature_dict[input_x[i][j]]=feature_num
                feature_num+=1
        feature.append(feature_dict)

    train_len=len(train_x)
    test_len=len(test_x)

    tr_x=[]
    te_x=[]

    for i in range(train_len):
        temp_x=[]
        for j in range(cols):
            temp_x.append(feature[j][x[i][j]])
        tr_x.append(temp_x)

    for i in range(test_len):
        temp_x=[]
        for j in range(cols):
            temp_x.append(feature[j][x[i][j]])
        te_x.append(temp_x)
    
    enc=OneHotEncoder()
    enc.fit(tr_x+te_x)

    return enc.transform(tr_x).toarray(), enc.transform(te_x).toarray()

def write_result(ty, y, output_file):
    ty=[100*x for x in ty]
    y=[100*x for x in y]
    f=open(output_file, 'w')
    for m, n in zip(ty, y):
        n=float('%.4f'%n)
        f.write(str(m)+'\t'+str(n)+'\n')
    f.close()

def draw(y1, y2):
    from matplotlib.font_manager import FontProperties
    import matplotlib.pyplot as plt
    font = FontProperties(fname=r"/usr/share/matplotlib/mpl-data/fonts/ttf/cmb10.ttf", size=14) 
    # from pylab import mpl
    # mpl.rcParams['font.sans-serif'] = ['SimHei']
    x=range(len(y1))
    x=x[1000:2000]
    x=[i-1000 for i in x]
    y1=y1[1000:2000]
    y1=[100*i for i in y1]
    y2=y2[1000:2000]
    y2=[100*i for i in y2]

    plt.plot(x, y1, 'b', x, y2, 'r')
    # plt.axis([0, 110, 0, 80])
    plt.grid(True)
    plt.xlabel('Test Sample Index')
    plt.ylabel('CTR(%)')
    plt.title(u'CTR Prediction Results (blue: real ctr, red: predicted ctr)')
    plt.show()


def lr_test(train_x, train_y, test_x, test_y):
    clf=linear_model.LogisticRegression(penalty='l2')
    clf.fit(train_x, train_y)

    y1=clf.predict_proba(train_x)
    y2=clf.predict_proba(test_x)

    y1=[x[-1] for x in y1]
    y2=[x[-1] for x in y2]

    # write_result(train_y, y1, 'y1')
    # write_result(test_y, y2, 'y2')
    # import pdb;pdb.set_trace()

    ymax=max(train_y)
    from random import random
    random_y=[random()*ymax for i in range(len(y1))]

    all_zero=[0]*len(y1)
    
    import numpy
    y_mean=numpy.mean(y1)
    y_std=numpy.std(y1)
    mu,sigma=y_mean, y_std
    # rarray=numpy.random.normal(mu,sigma,len(y1))
    # rarray=[ numpy.random.normal(x,sigma,1) for x in range(len(y1))]
    rarray=numpy.random.normal(0,0.002,len(y1))
    # write_result(train_y, rarray, 'xxx')
    # draw(train_y, rarray+train_y)

    re_y=[]
    for i in range(len(train_y)):
        if train_y[i]+rarray[i]<0:
            re_y.append(0)
        else:
            re_y.append(train_y[i]+rarray[i])

    zip_train_y=[]
    for a, b in zip(train_y, re_y):
        zip_train_y.append([a, b])

    # import pdb;pdb.set_trace()
    zip_train_y=sorted(zip_train_y, key=lambda x:x[0])
    zip_y1=[]
    zip_y2=[]
    for ele in zip_train_y:
        zip_y1.append(ele[0])
        zip_y2.append(ele[1])
    draw(zip_y1, zip_y2)



    # llfun(train_y, y1)
    # llfun(test_y, y2)

    # llfun(train_y, random_y)
    # llfun(test_y, random_y)
    
    # llfun(train_y, all_zero)
    # llfun(test_y, all_zero)

    # mae(train_y, y1)
    # mae(test_y, y2)

    # mae(train_y, random_y)
    # mae(test_y, random_y)
    
    # mae(train_y, all_zero)
    # mae(test_y, all_zero)

if __name__=="__main__":
    train_x, train_y=read_vec('train_data')
    test_x, test_y=read_vec('test_data')

    # train_y=[10*x for x in train_y]
    # test_y=[10*x for x in test_y]

    # import pdb;pdb.set_trace()
    train_x, test_x=vectorlize(train_x, test_x)
    lr_test(train_x, train_y, test_x, test_y)