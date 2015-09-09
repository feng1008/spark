#!/usr/bin/python
#-*- coding:utf8 -*-
import codecs
import os
import pickle
from sklearn import linear_model
from sklearn import cross_validation
from sklearn.feature_extraction.text import CountVectorizer,TfidfTransformer

ALPHA=0.8
app_vocabulary=set()

def gen_vocab(filename):
    global app_vocabulary

    for lines in open(filename, 'r').xreadlines():
        app_vocabulary.add(lines.strip('\r\n').replace('.', '_'))

def gen_label(filename):
    x=[]
    y=[]

    for lines in open(filename, 'r').xreadlines():
        sex, package_str=lines.strip('\r\n').split('\t')
        x.append(package_str.replace('|', ' ').replace('.', '_'))
        y.append(int(sex))
    return x, y

def vectorize_data(train_data, test_data):
    global app_vocabulary
    
    vectorize=CountVectorizer(vocabulary=list(app_vocabulary))
    counts_train=vectorize.fit_transform(train_data)
    counts_test=vectorize.fit_transform(test_data)

    # f=open('model/vector.pkl','w')
    # pickle.dump(vectorize, f)

    return counts_train, counts_test

def calculate_mse(p_y, t_y):
    if len(p_y)!=len(t_y):
        return -1
    y_len=len(p_y)
    p_y=[1 if x>0.5 else 0 for x in p_y]
    error=sum([1 if p_y[i]!=t_y[i] else 0 for i in range(y_len)])
    # error=sum([(p_y[i]-t_y[i])**2 for i in xrange(y_len)])
    t_y_female=sum(t_y)
    t_y_male=len(t_y)-t_y_female
    print 'male:'+str(t_y_male)+','+'female:'+str(t_y_female)
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
    print 'ma->ma:'+str(result[0])+'\t'+'ma->fe:'+str(result[1])+'\t'+'fe->ma:'+str(result[2])+'\t'+'fe->fe:'+str(result[3])

    print str(error/float(y_len))

def lr_l1_test(train_x, train_y, test_x, test_y):
    print "logistic regression loss with L1 norm:"  
    if not os.path.exists('model/lr_l1_clf.pkl'):
        clf = linear_model.LogisticRegression(penalty='l1', tol=0.1, C=1)
        clf.fit(train_x, train_y)

        output=open('model/lr_l1_clf.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/lr_l1_clf.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict(train_x)    
    # print str(y1[2])
    y2=clf.predict(test_x)
    # import pdb;pdb.set_trace()
    calculate_mse(y1, train_y)        #0.198478006668
    calculate_mse(y2, test_y)

def write_predict(prob, output_file):
    f=open(output_file, 'w')
    for x in prob:
        f.write(str(x)+'\n')
    f.close()

def lr_l2_test(train_x, train_y, test_x, test_y):
    print "logistic regression loss with L2 norm:"  
    if not os.path.exists('model/lr_l2_clf.pkl'):
        clf = linear_model.LogisticRegression(penalty='l2', tol=0.1, C=1)
        # import pdb;pdb.set_trace()
        # train_x, train_y, test_x, test_y=cross_validation.train_test_split(train_x+test_x,train_y+test_y, test_size=0.4, random_state=0)
        # lr_scores = cross_validation.cross_val_score(clf, train_x, train_y, cv=5)
        # print lr_scores
        clf.fit(train_x, train_y)

        output=open('model/lr_l2_clf.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/lr_l2_clf.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict_proba(train_x)
    # print str(y1[2])
    y2=clf.predict_proba(test_x)
    y2=[x[-1] for x in y2]
    # import pdb;pdb.set_trace()
    write_predict(y2, 'y.txt')
    # calculate_mse(y1, train_y)
    # calculate_mse(y2, test_y)
    return y2

def svc_test(train_x, train_y, test_x, test_y):
    print "SVM:" 
    from sklearn.svm import SVR
    if not os.path.exists('model/svm.pkl'):
        clf =SVR()
        # cross_validation.cross_val_score(clf, train_x, train_y, cv=10)
        clf.fit(train_x, train_y)

        output=open('model/svm.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/svm.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict(train_x)
    y2=clf.predict(test_x)
    calculate_mse(y1, train_y)
    calculate_mse(y2, test_y)

def rf_test(train_x, train_y, test_x, test_y):
    print "random forest:" 
    from sklearn.ensemble import RandomForestRegressor
    if not os.path.exists('model/rf.pkl'):
        clf = RandomForestRegressor(n_estimators=10, max_depth=None, min_samples_split=1, random_state=0)
        clf.fit(train_x.toarray(), train_y)

        output=open('model/rf.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/rf.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict(train_x.toarray())
    y2=clf.predict(test_x.toarray())
    calculate_mse(y1, train_y)
    calculate_mse(y2, test_y)

def GBDT_test(train_x, train_y, test_x, test_y):
    print "Gradient Boosting Classifier:" 
    from sklearn.ensemble import GradientBoostingRegressor
    if not os.path.exists('model/GBDT.pkl'):
        clf = GradientBoostingRegressor(n_estimators=30, learning_rate=1.0, max_depth=1, random_state=0)
        clf.fit(train_x.toarray(), train_y)

        output=open('model/GBDT.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/GBDT.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict(train_x.toarray())
    y2=clf.predict(test_x.toarray())
    calculate_mse(y1, train_y)
    calculate_mse(y2, test_y)

def backpro(x, y):
    app_prob_dict={}
    app_num_dict={}
    if len(x)!=len(y):
        return
    for apps, prob in zip(x,y):
        # import pdb;pdb.set_trace()
        # ap=apps.strip('\r\n').split('\t')[-1].split('|')
        ap=apps.split(' ')
        for t in ap:
            if app_prob_dict.has_key(t):
                app_prob_dict[t]+=prob
                app_num_dict[t]+=1
            else:
                app_prob_dict[t]=prob
                app_num_dict[t]=1
    
    for k,v in app_prob_dict.items():
        app_prob_dict[k]=app_prob_dict[k]/float(app_num_dict[k])

    result=[]
    for apps, prob in zip(x,y):
        # ap=apps.strip('\r\n').split('\t')[-1].split('|')
        ap=apps.split(' ')
        ap_len=len(ap)
        b_y=0.0
        for t in ap:
            b_y+=app_prob_dict[t]
        b_y=ALPHA*prob+(1-ALPHA)*b_y/ap_len
        result.append(b_y)
    return result

if __name__=='__main__':
    gen_vocab('data/aos_top3w')
    train_app, train_y=gen_label('data/train')
    test_app, test_y=gen_label('data/test')

    train_x, test_x =vectorize_data(train_app, test_app)

    # lr_l1_test(train_x, train_y, test_x, test_y)
    y=lr_l2_test(train_x, train_y, test_x, test_y)
    result=backpro(test_app, y)
    write_predict(result, 'result.txt')
    # svc_test(train_x, train_y, test_x, test_y)
    # rf_test(train_x, train_y, test_x, test_y)
    # GBDT_test(train_x, train_y, test_x, test_y)