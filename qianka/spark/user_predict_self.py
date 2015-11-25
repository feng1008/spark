#!/usr/bin/python
#-*- coding:utf8 -*-
import codecs
import os
import pickle
from sklearn import linear_model
import scipy as sp

app_vocabulary=set()

def data_filter(filename):
    f=codecs.open(filename, 'r', 'utf-8')
    result=[]
    count=0
    for lines in f.readlines():
        try:
            name, package_str=lines.strip('\r\n').split('\t')
            # result.append(package_str.split('|'))
            result.append(package_str.replace('|',' '))
            count+=1
        except:
            continue
    f.close()
    return result,count

def read_data(filename):
    global app_vocabulary

    f=codecs.open(filename, 'r', 'utf-8')
    result=[]
    count=0
    for lines in f.readlines():
        try:
            app_vocabulary |=set(lines.strip('\r\n').split(' '))
            result.append(lines.strip('\r\n').replace('.', '_'))
            count+=1
        except:
            continue
    f.close()
    return result,count

# def vectorize_data(train_data):
#     from sklearn.feature_extraction.text import CountVectorizer,TfidfTransformer  
#     count_v= CountVectorizer()
#     # import pdb;pdb.set_trace()
#     counts_train = count_v.fit_transform(train_data)
#     print "the shape of train is "+repr(counts_train.shape)  
      
#     tfidftransformer=TfidfTransformer()
#     tfidf_train=tfidftransformer.fit(counts_train).transform(counts_train)

#     return tfidf_train

def save_vocabulary(vocab, filename):
    f=codecs.open(filename, 'w', 'utf-8')
    for v in vocab:
        f.write(v+'\n')
    f.close()

def vectorize_data(train_data, test_data):
    global app_vocabulary

    from sklearn.feature_extraction.text import CountVectorizer,TfidfTransformer  
    vocabulary=[x.replace('.', '_') for x in list(app_vocabulary)]
    save_vocabulary(vocabulary, 'vocabulary.txt')
    # train_data=[x.replace('.','_') for x in train_data]
    # test_data=[x.replace('.','_') for x in test_data]

    count_v1= CountVectorizer(vocabulary=vocabulary)
    # import pdb;pdb.set_trace()
    counts_train = count_v1.fit_transform(train_data)
    # print "the shape of train is "+repr(counts_train.shape)  
    
    count_v2 = CountVectorizer(vocabulary=count_v1.vocabulary_)
    counts_test = count_v2.fit_transform(test_data)

    save_vocabulary(count_v2.get_feature_names(), 'vocabulary.txt')

    tfidftransformer=TfidfTransformer()
    tfidf_train=tfidftransformer.fit(counts_train).transform(counts_train)
    tfidf_test=tfidftransformer.fit(counts_test).transform(counts_test)

    return tfidf_train, tfidf_test

def calculate_mse(p_y, t_y):
    if len(p_y)!=len(t_y):
        return -1
    y_len=len(p_y)
    # mse_error=sum([(t_y[i]-p_y[i])**2 for i in range(y_len)])/float(y_len)
    # print str(mse_error)
    # p_y=[1 if y>0.5 else 0 for y in p_y]
    # error=sum([1 if p_y[i]!=t_y[i] else 0 for i in range(y_len)])
    error=sum([1 if p_y[i]==0 and t_y[i]==1 else 0 for i in range(y_len)])
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
    if not os.path.exists('lasso_clf.pkl'):
        clf = linear_model.Lasso(alpha = 0.1)
        clf.fit(train_x, train_y)
        
        # print str(clf.score(train_x, labels))
        output = open('lasso_clf.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('lasso_clf.pkl', 'rb')
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
    if not os.path.exists('ridge_clf.pkl'):
        clf = linear_model.Ridge (alpha = .5)
        clf.fit(train_x, train_y)
        
        # print str(clf.score(train_x, labels))
        output = open('ridge_clf.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('ridge_clf.pkl', 'rb')
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

def lr_test(train_x, train_y, test_x, test_y):
    print "logistic regression loss:"  
    if not os.path.exists('lr_clf.pkl'):
        clf = linear_model.LogisticRegression(penalty='l1')
        clf.fit(train_x, train_y)

        output=open('lr_clf.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('lr_clf.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict(train_x)    
    # print str(y1[2])
    y2=clf.predict(test_x)
    calculate_mse(y1, train_y)        #0.198478006668
    calculate_mse(y2, test_y)
    llfun(y1,train_y)
    llfun(y2,test_y)

    # error=sum([1 if test_y[i]!=y2[i] else 0 for i in range(len(test_y))])
    # error=sum([1 if test_y[i]==0 and y2[i]==1 else 0 for i in range(len(test_y))])
    # print str(error)

# def svc_test(train_x, labels):
#     from sklearn.svm import SVC
#     clf = SVC()
#     clf.fit(train_x, labels)
#     y=clf.predict(train_x)
#     calculate_mse(y, labels)
#     output=open('svc_clf.pkl', 'wb')
#     pickle.dump(clf,output)

def purify(negative_train, train_x, labels, purify_output):
    output=open('ridge_clf.pkl', 'rb')
    clf=pickle.load(output)
    y=clf.predict(train_x)

    f=codecs.open(purify_output, 'w', 'utf-8')
    for i in range(len(y)/2, len(y)):
        if y[i]<0.1 and labels[i]==0:
            # import pdb;pdb.set_trace()
            f.write(negative_train[i-len(y)/2]+'\n')
    f.close()

def repurify_positive(input_file, output_file):
    f1=codecs.open(input_file, 'r', 'utf-8')
    f2=codecs.open(output_file, 'w', 'utf-8')
    for lines in f1.readlines():
        name, package_str=lines.strip('\r\n').split('\t')
        f2.write(package_str.replace('|', ' ')+'\n')
    f1.close()
    f2.close()

def feature_selection(model_file, vocabulary_file):
    output=open(model_file, 'rb')
    clf=pickle.load(output)
    vocabulary=[]

    f=codecs.open(vocabulary_file, 'r', 'utf-8')
    for lines in f.readlines():
        vocabulary.append(lines.strip('\r\n'))
    f.close()

    print len(vocabulary)

    print len(clf.coef_)

if __name__=='__main__':
    # repurify_positive('LR/positive2', 'LR/positive')
    # positive_train, positive_len=data_filter('LR/positive_train')
    # negative_train, negative_len=data_filter('LR/negative_train')

    positive_train, positive_train_len=read_data('LR/positive_train')
    negative_train, negative_train_len=read_data('LR/negative_train')
    train_data=positive_train+negative_train
    train_y=[1]*positive_train_len+[0]*negative_train_len

    positive_test, positive_test_len=read_data('LR/positive_test')
    negative_test, negative_test_len=read_data('LR/negative_test')
    test_data=positive_test+negative_test
    test_y=[1]*positive_test_len+[0]*negative_test_len

    train_x, test_x =vectorize_data(train_data, test_data)

    sgd_test(train_x, train_y, test_x, test_y)
    ridge_test(train_x, train_y, test_x, test_y)
    lr_test(train_x, train_y, test_x, test_y)
    # lasso_test(train_x, train_y, test_x, test_y)

    # feature_selection('lr_clf.pkl', 'vocabulary.txt')
    # svc_test(train_x, labels)
    # purify(negative_train, train_x, labels, 'LR/purify_output')

