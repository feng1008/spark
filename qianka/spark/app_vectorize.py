#!/usr/bin/python
#-*- coding:utf8 -*-
import codecs
import os
import pickle
from sklearn import linear_model
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier
import scipy as sp

app_vocabulary=set()
general_app=set()

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
    p_y=[1 if x>0.5 else 0 for x in p_y]
    error=sum([1 if p_y[i]!=t_y[i] else 0 for i in range(y_len)])
    # error=sum([(p_y[i]-t_y[i])**2 for i in xrange(y_len)])
    t_y1=sum(t_y)
    t_y0=len(t_y)-t_y1
    print '0:'+str(t_y0)+','+'1:'+str(t_y1)
    result=[0]*4
    for p_ele, t_ele in zip(p_y,t_y):
        if p_ele==t_ele:
            if p_ele==0:
                result[0]+=1
            else:
                result[3]+=1
        else:
            if p_ele==1:
                result[1]+=1
            else:
                result[2]+=1
    print '0->0:'+str(result[0])+'\t'+'0->1:'+str(result[1])+'\t'+'1->0:'+str(result[2])+'\t'+'1->1:'+str(result[3])

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

    y1=clf.predict_proba(train_x)    
    # print str(y1[2])
    y2=clf.predict_proba(test_x)
    y1=[x[-1] for x in y1]
    y2=[x[-1] for x in y2]
    calculate_mse(y1, train_y)
    calculate_mse(y2, test_y)

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

    y1=clf.predict_proba(train_x)    
    y2=clf.predict_proba(test_x)
    y1=[x[-1] for x in y1]
    y2=[x[-1] for x in y2]
    calculate_mse(y1, train_y)        #0.198478006668
    calculate_mse(y2, test_y)
    return y1,y2

def svc_test(train_x, train_y, test_x, test_y):
    print "SVM:" 
    from sklearn.svm import SVC
    if not os.path.exists('model/SVM.pkl'):
        clf =SVC()
        clf.fit(train_x, train_y)

        output=open('model/SVM.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/SVM.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict(train_x)    
    y2=clf.predict(test_x)
    calculate_mse(y1, train_y)        #0.198478006668
    calculate_mse(y2, test_y)

def GBDT_test(train_x, train_y, test_x, test_y):
    print "gradient boosting classifier:"  
    if not os.path.exists('model/GBDT.pkl'):
        # import pdb;pdb.set_trace()
        est=GradientBoostingClassifier(n_estimators=50, learning_rate=0.1, max_depth=1, random_state=0, loss='deviance')
        est.fit(train_x, train_y)

        output=open('model/GBDT.pkl', 'wb')
        pickle.dump(est,output)
    else:
        output=open('model/GBDT.pkl', 'rb')
        est=pickle.load(output)

    y1=est.predict(train_x)    
    # print str(y1[2])
    y2=est.predict(test_x)
    calculate_mse(y1, train_y)        #0.198478006668
    calculate_mse(y2, test_y)

def RF_test(train_x, train_y, test_x, test_y):
    print "random forest classifier:"  
    if not os.path.exists('model/RF.pkl'):
        est=RandomForestClassifier(n_estimators=3, criterion='gini')
        est.fit(train_x, train_y)

        output=open('model/RF.pkl', 'wb')
        pickle.dump(est,output)
    else:
        output=open('model/RF.pkl', 'rb')
        est=pickle.load(output)

    y1=est.predict(train_x)    
    y2=est.predict(test_x)
    calculate_mse(y1, train_y)
    calculate_mse(y2, test_y)

# def svc_test(train_x, labels):
#     from sklearn.svm import SVC
#     clf = SVC()
#     clf.fit(train_x, labels)
#     y=clf.predict(train_x)
#     calculate_mse(y, labels)
#     output=open('svc_clf.pkl', 'wb')
#     pickle.dump(clf,output)

# def repurify_positive(input_file, output_file):
#     f1=codecs.open(input_file, 'r', 'utf-8')
#     f2=codecs.open(output_file, 'w', 'utf-8')
#     for lines in f1.readlines():
#         name, package_str=lines.strip('\r\n').split('\t')
#         f2.write(package_str.replace('|', ' ')+'\n')
#     f1.close()
#     f2.close()

# def save_coef(coef, filename):
#     f=codecs.open(filename, 'w', 'utf-8')
#     for c_rows in coef:
#         for i in c_rows:
#             f.write(str(i)+'\n')
#     f.close()

# def feature_selection(model_file, vocabulary_file):
#     global app_vocabulary

#     f=codecs.open(vocabulary_file, 'w', 'utf-8')

#     output=open(model_file, 'rb')
#     clf=pickle.load(output)

#     coef=clf.coef_[0]
#     vocabulary=list(app_vocabulary)

#     for a, b in zip(coef, vocabulary):
#         if a!=0.0:
#             f.write(str(a)+'\t'+b)
#     f.close()
#     # save_coef(clf.coef_, vocabulary_file)

def catvec(x,y):
    if len(x)!=len(y):
        print "The length of vectors are not the same!"
        return
    result=[]
    for a, b in zip(x, y):
        result.append(a+[b])
    return result

def cat_feature(filename, y1, y2):
    train_x=[]
    test_x=[]
    count=0
    y1_len=len(y1)
    for lines in open(filename, 'r').readlines():
        # import pdb;pdb.set_trace()
        gen_rat, fee_count, fee_avg=lines.strip('\r\n').split('\t')
        if count<y1_len:
            y=y1[count]
            train_x.append([float(gen_rat), float(fee_count), float(fee_avg), y])
        else:
            y=y2[count-y1_len]
            test_x.append([float(gen_rat), float(fee_count), float(fee_avg), y])
        count+=1
    return train_x, test_x

if __name__=='__main__':
    # repurify_positive('LR/positive2', 'LR/positive')

    gen_vocab('data_0831/ios_top3w')
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
    # ridge_test(train_x, train_y, test_x, test_y)
    # lr_l1_test(train_x, train_y, test_x, test_y)
    y1, y2=lr_l2_test(train_x, train_y, test_x, test_y)
    # lasso_test(train_x, train_y, test_x, test_y)

    # GBDT_test(train_x, train_y, test_x, test_y)
    # svc_test(train_x, train_y, test_x, test_y)

    # feature_selection('lr_clf.pkl', 'coef.txt')
    # svc_test(train_x, labels)
    train_x2, test_x2=cat_feature('data_0831/add_feature', y1, y2)
    ridge_test(train_x2, train_y, test_x2, test_y)
    lr_l1_test(train_x2, train_y, test_x2, test_y)
    GBDT_test(train_x2, train_y, test_x2, test_y)
    # svc_test(train_x, train_y, test_x, test_y)
    RF_test(train_x, train_y, test_x, test_y)