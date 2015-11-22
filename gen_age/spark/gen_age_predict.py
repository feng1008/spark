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
        app_vocabulary.add(lines.strip('\r\n').split('\t')[0].replace('.', '_'))

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

    tfidftransformer = TfidfTransformer();
    counts_train=tfidftransformer.fit(counts_train).transform(counts_train);
    counts_test=tfidftransformer.fit(counts_test).transform(counts_test);

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
        cs=[0.01, 0.05, 0.1, 0.2, 0.5, 2.0, 5.0, 10.0]
        clf = linear_model.LogisticRegressionCV(Cs=cs, cv=10, penalty='l2', tol=0.1, max_iter=30)
        clf.fit(train_x, train_y)

        output=open('model/lr_l1_clf.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/lr_l1_clf.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict_proba(train_x)
    # print str(y1[2])
    y2=clf.predict_proba(test_x)
    y2=[x[-1] for x in y2]
    # import pdb;pdb.set_trace()
    write_predict(y2, 'y.txt')
    # calculate_mse(y1, train_y)
    calculate_mse(y2, test_y)
    return y2

def write_predict(prob, output_file):
    f=open(output_file, 'w')
    for x in prob:
        f.write(str(x)+'\n')
    f.close()

def lr_l2_test(train_x, train_y, test_x, test_y):
    print "logistic regression loss with L2 norm:"  
    if not os.path.exists('model/lr_l2_clf.pkl'):
        cs=[0.01, 0.05, 0.1, 0.2, 0.5, 2.0, 5.0, 10.0]
        clf = linear_model.LogisticRegressionCV(Cs=cs, cv=10, penalty='l2', tol=0.1, max_iter=30)
        # import pdb;pdb.set_trace()
        # train_x, train_y, test_x, test_y=cross_validation.train_test_split(train_x+test_x, train_y+test_y, test_size=0.4, random_state=0)
        # lr_scores = cross_validation.cross_val_score(clf, train_x, train_y, cv=5)
        # print lr_scores
        clf.fit(train_x, train_y)

        output=open('model/lr_l2_clf.pkl', 'wb')
        pickle.dump(clf,output)
    else:
        output=open('model/lr_l2_clf.pkl', 'rb')
        clf=pickle.load(output)

    y1=clf.predict_proba(train_x)
    y1=[x[-1] for x in y1]
    # print str(y1[2])
    y2=clf.predict_proba(test_x)
    y2=[x[-1] for x in y2]
    # import pdb;pdb.set_trace()
    write_predict(y2, 'y.txt')
    calculate_mse(y1, train_y)
    calculate_mse(y2, test_y)
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
    return y2

def backpro(x, y, test_y):
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
    calculate_mse(result, test_y)
    return result

if __name__=='__main__':
    gen_vocab('data/aos_install_top3w')
    train_app, train_y=gen_label('data/install_train')
    test_app, test_y=gen_label('data/install_test')

    train_x, test_x =vectorize_data(train_app, test_app)

    # y=lr_l1_test(train_x, train_y, test_x, test_y)
    # y=lr_l2_test(train_x, train_y, test_x, test_y)
    y=GBDT_test(train_x, train_y, test_x, test_y)
    # import pdb;pdb.set_trace()
    result=backpro(test_app, y, test_y)
    write_predict(result, 'result.txt')
    # svc_test(train_x, train_y, test_x, test_y)
    # rf_test(train_x, train_y, test_x, test_y)