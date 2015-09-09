#!/usr/bin/python
#-*- coding:utf8 -*-
import codecs
import os
import pickle
from sklearn import linear_model
from sklearn.ensemble import GradientBoostingRegressor
import scipy as sp

app_vocabulary=set()

def gen_vocab(filename):
    global app_vocabulary

    for lines in open(filename, 'r').xreadlines():
        app_vocabulary.add(lines.strip('\r\n').replace('.', '_'))

def read_data(filename):
    global app_vocabulary

    f=codecs.open(filename, 'r', 'utf-8')
    result=[]
    count=0
    for lines in f.readlines():
        try:
            name, package_str=lines.strip('\r\n').split('\t')
            # app_vocabulary |=set(package_str.split('|'))
            result.append(package_str.replace('|',' ').replace('.', '_'))
            count+=1
        except:
            continue
    f.close()
    return result,count

def save_vocabulary(vocab, filename):
    f=codecs.open(filename, 'w', 'utf-8')
    for v in vocab:
        f.write(v+'\n')
    f.close()

def vectorize_data(train_data, test_data):
    global app_vocabulary

    from sklearn.feature_extraction.text import CountVectorizer,TfidfTransformer
    # save_vocabulary(app_vocabulary, 'vocabulary.txt')
    # vocabulary=[x.replace('.', '_') for x in list(app_vocabulary)]
    pickle.dump(app_vocabulary, open('model/vocabulary.pkl','w'))
    # train_data=[x.replace('.','_') for x in train_data]
    # test_data=[x.replace('.','_') for x in test_data]

    vectorize=CountVectorizer(vocabulary=list(app_vocabulary))
    counts_train=vectorize.fit_transform(train_data)
    counts_test=vectorize.fit_transform(test_data)

    f=open('model/vector.pkl','w')
    pickle.dump(vectorize, f)

    return counts_train, counts_test

def calculate_mse(p_y, t_y):
    if len(p_y)!=len(t_y):
        return -1
    y_len=len(p_y)
    # mse_error=sum([(t_y[i]-p_y[i])**2 for i in range(y_len)])/float(y_len)
    # print str(mse_error)
    # # p_y=[1 if y>0.5 else 0 for y in p_y]
    error=sum([1 if p_y[i]!=t_y[i] else 0 for i in range(y_len)])
    # error=sum([1 if p_y[i]==0 and t_y[i]==1 else 0 for i in range(y_len)])
    print str(error/float(y_len))

def llfun(pred, act):
    epsilon = 1e-15
    pred = sp.maximum(epsilon, pred)
    pred = sp.minimum(1-epsilon, pred)
    ll = sum(act*sp.log(pred) + sp.subtract(1,act)*sp.log(sp.subtract(1,pred)))
    ll = ll * -1.0/len(act)
    print str(ll)

def sgd_test(train_x, train_y, test_x, test_y):
    print "sgd elasticnet loss:"  
    if not os.path.exists('sgd_clf.pkl'):
        clf = linear_model.SGDRegressor(loss='squared_loss', penalty='elasticnet', n_iter=5, random_state=True)
        clf.fit(train_x, train_y)

        output = open('sgd_clf.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('sgd_clf.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict(train_x)
    y1=[1 if y>0.5 else 0  for y in y1] 
    # print str(y1[2])
    y2=clf.predict(test_x)
    y2=[1 if y>0.5 else 0  for y in y2]
    calculate_mse(y1, train_y)        #0.502772067591  
    calculate_mse(y2, train_y)
    llfun(y1,train_y)
    llfun(y2,test_y)

def lasso_test(train_x, train_y, test_x, test_y):
    print "lasso regression loss:"  
    if not os.path.exists('model/lasso_clf.pkl'):
        clf = linear_model.Lasso(alpha = 0.1)
        clf.fit(train_x, train_y)
        
        # print str(clf.score(train_x, labels))
        output = open('model/lasso_clf.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/lasso_clf.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict(train_x)   
    y1=[1 if y>0.5 else 0  for y in y1] 
    # print str(y1[2])
    y2=clf.predict(test_x)  
    y2=[1 if y>0.5 else 0  for y in y2] 
    calculate_mse(y1, train_y)        
    calculate_mse(y2, test_y)
    llfun(y1,train_y)
    llfun(y2,test_y)

def ridge_test(train_x, train_y, test_x, test_y):
    print "ridge regression loss:"  
    if not os.path.exists('model/ridge_clf.pkl'):
        clf = linear_model.Ridge (alpha = .5)
        clf.fit(train_x, train_y)
        
        # print str(clf.score(train_x, labels))
        output = open('model/ridge_clf.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/ridge_clf.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict(train_x)   
    y1=[1 if y>0.5 else 0  for y in y1] 
    # print str(y1[2])
    y2=clf.predict(test_x)  
    y2=[1 if y>0.5 else 0  for y in y2] 
    calculate_mse(y1, train_y)        #0.147935749904
    calculate_mse(y2, test_y)
    llfun(y1,train_y)
    llfun(y2,test_y)

def save_linear_para(clf, filename):
    d={}
    d['coef']=clf.coef_[0]
    d['intercept']=clf.intercept_
    pickle.dump(d, open(filename, 'w'))

def lr_l1_test(train_x, train_y, test_x, test_y):
    print "logistic regression loss with L1 norm:"  
    if not os.path.exists('model/lr_l1_clf.pkl'):
        clf = linear_model.LogisticRegression(penalty='l1')
        clf.fit(train_x, train_y)

        output=open('model/lr_l1_clf.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/lr_l1_clf.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict(train_x)    
    # print str(y1[2])
    y2=clf.predict(test_x)
    calculate_mse(y1, train_y)        #0.198478006668
    calculate_mse(y2, test_y)
    llfun(y1,train_y)
    llfun(y2,test_y)

def lr_l2_test(train_x, train_y, test_x, test_y):
    print "logistic regression loss with L2 norm:"  
    if not os.path.exists('model/lr_l2_clf.pkl'):
        clf = linear_model.LogisticRegression(penalty='l2')
        clf.fit(train_x, train_y)

        output=open('model/lr_l2_clf.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/lr_l2_clf.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict(train_x)    
    # print str(y1[2])
    y2=clf.predict(test_x)
    calculate_mse(y1, train_y)        #0.198478006668
    calculate_mse(y2, test_y)
    llfun(y1,train_y)
    llfun(y2,test_y)

def svc_test(train_x, train_y, test_x, test_y):
    print "SVM:" 
    from sklearn.svm import SVC
    if not os.path.exists('model/svm.pkl'):
        clf =SVC()
        clf.fit(train_x, train_y)

        output=open('model/svm.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/svm.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict(train_x)    
    # print str(y1[2])
    y2=clf.predict(test_x)
    calculate_mse(y1, train_y)        #0.198478006668
    calculate_mse(y2, test_y)
    llfun(y1,train_y)
    llfun(y2,test_y)

def GBDT_test(train_x, train_y, test_x, test_y):
    print "gradient boosting regression:"  
    if not os.path.exists('model/GBDT.pkl'):
        est=GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, max_depth=1, random_state=0, loss='ls')
        est.fit(train_x.toarray, train_y)

        output=open('model/GBDT.pkl', 'wb')
        pickle.dump(est,output)
    else:
        output=open('model/GBDT.pkl', 'rb')
        est=pickle.load(output)

    y1=est.predict(train_x.toarray)    
    # print str(y1[2])
    y2=est.predict(test_x.toarray)
    calculate_mse(y1, train_y)        #0.198478006668
    calculate_mse(y2, test_y)
    llfun(y1,train_y)
    llfun(y2,test_y)

# def svc_test(train_x, labels):
#     from sklearn.svm import SVC
#     clf = SVC()
#     clf.fit(train_x, labels)
#     y=clf.predict(train_x)
#     calculate_mse(y, labels)
#     output=open('svc_clf.pkl', 'wb')
#     pickle.dump(clf,output)

# def purify(negative_train, train_x, labels, purify_output):
#     output=open('model/ridge_clf.pkl', 'rb')
#     clf=pickle.load(output)
#     y=clf.predict(train_x)

#     f=codecs.open(purify_output, 'w', 'utf-8')
#     for i in range(len(y)/2, len(y)):
#         if y[i]<0.1 and labels[i]==0:
#             # import pdb;pdb.set_trace()
#             f.write(negative_train[i-len(y)/2]+'\n')
#     f.close()

def repurify_positive(input_file, output_file):
    f1=codecs.open(input_file, 'r', 'utf-8')
    f2=codecs.open(output_file, 'w', 'utf-8')
    for lines in f1.readlines():
        name, package_str=lines.strip('\r\n').split('\t')
        f2.write(package_str.replace('|', ' ')+'\n')
    f1.close()
    f2.close()

def save_coef(coef, filename):
    f=codecs.open(filename, 'w', 'utf-8')
    for c_rows in coef:
        for i in c_rows:
            f.write(str(i)+'\n')
    f.close()

def feature_selection(model_file, vocabulary_file):
    global app_vocabulary

    f=codecs.open(vocabulary_file, 'w', 'utf-8')

    output=open(model_file, 'rb')
    clf=pickle.load(output)

    coef=clf.coef_[0]
    vocabulary=list(app_vocabulary)

    for a, b in zip(coef, vocabulary):
        if a!=0.0:
            f.write(str(a)+'\t'+b)
    f.close()
    # save_coef(clf.coef_, vocabulary_file)

if __name__=='__main__':
    # repurify_positive('LR/positive2', 'LR/positive')

    positive_train, positive_train_len=read_data('data_0831/pos_train')
    negative_train, negative_train_len=read_data('data_0831/neg_train')
    train_data=positive_train+negative_train
    train_y=[1]*positive_train_len+[0]*negative_train_len

    positive_test, positive_test_len=read_data('data_0831/pos_test')
    negative_test, negative_test_len=read_data('data_0831/neg_test')
    test_data=positive_test+negative_test
    test_y=[1]*positive_test_len+[0]*negative_test_len

    train_x, test_x =vectorize_data(train_data, test_data)

    # sgd_test(train_x, train_y, test_x, test_y)
    ridge_test(train_x, train_y, test_x, test_y)
    lr_l1_test(train_x, train_y, test_x, test_y)
    lr_l2_test(train_x, train_y, test_x, test_y)
    # lasso_test(train_x, train_y, test_x, test_y)

    # GBDT_test(train_x, train_y, test_x, test_y)
    # svc_test(train_x, train_y, test_x, test_y)

    # feature_selection('lr_clf.pkl', 'coef.txt')
    # svc_test(train_x, labels)