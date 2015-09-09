#!/usr/bin/python
#-*- coding:utf8 -*-
import urllib
import sys
import codecs
import csv
import os

app_pay_dict={}

def getfrom_api(packages):
    myurl = "https://itunes.apple.com/lookup?bundleId=" + packages +"&country=cn"
    urlText = urllib.urlopen(myurl)
    content = urlText.read()
    #tmp = json.loads(content)
    return content.replace(':false',':0').replace(':true',':1').replace('\r\n','').replace('\n','').strip()

def get_package_set(inputFile, outputFile):
    '''
        get the set of packages
        get_package_set('user_package', 'packages')
    '''
    package_set=set()
    f1=open(inputFile,'r')
    f2=open(outputFile, 'w')
    for lines in f1.readlines():
        ymid, packages=lines.strip('\r\n').split('\t')
        pack_set=set(packages.split('|'))
        package_set|=pack_set
    f1.close()

    temp=list(package_set)
    for t in temp:
        f2.write(t+'\n')
    f2.close()

def get_json_info():
    '''
        read the set of packages to get the json file
        get_json_info('packages', 'packages_json')

    '''
    f1=open(sys.argv[1],"r")
    f2=open(sys.argv[2],"w")
    count=1
    num=1
    for package in f1.readlines():
        try:
            app_json = getfrom_api(package.strip('\r\n'))
            f2.write(package.strip('\r\n')+'\t'+app_json+'\n')
        except:
            continue
        if count==50:
            print str(num*count)+" packages finished!\n"
            num=num+1
            count=1
        else:
            count+=1
    f1.close()
    f2.close()

def get_app_price(inputFile, outputFile):
    '''
        get the price of each package
        get_app_price('packages_json', 'packages_price')
    '''
    global app_pay_dict

    f1=open(inputFile, 'r')
    f2=open(outputFile, 'w')
    for lines in f1.readlines(): 
        # import pdb;pdb.set_trace()
        try:
            name, json_str=lines.strip('\r\n').split('\t')
            price=float(eval(json_str)['results'][0]['price'])
        except:
            continue
        
        if price>0.0:
            # app_class=eval(json_str)['results'][0]['genres']
            # app_pay_dict[name]={'price':price, 'app_class':app_class}
            app_pay_dict[name]=price
            f2.write(name+'\t'+str(price)+'\n')
        else:
            continue
    print len(app_pay_dict)
    f1.close()
    f2.close()

def get_user_info(package_file, user_info_file):
    '''
        get the app price of each user, filter the free packages
        get_user_info('user_package', 'user_package_info')
    '''
    global app_pay_dict

    f1=codecs.open(package_file, 'r', 'utf-8')
    f2=codecs.open(user_info_file, 'w', 'utf-8')
    for lines in f1.readlines():
        try:
            name, package_str=lines.strip('\r\n').split('\t')
        except:
            continue
        result=name+'\t'
        for app in package_str.split('|'):
            a_c_str=''
            if app_pay_dict.has_key(app):
                price=app_pay_dict[app]
                # price=app_pay_dict[app]['price']
                # app_class=app_pay_dict[app]['app_class']
                # for a_c in app_class:
                #     if app_class_dict.has_key(a_c):
                #         app_class_dict[a_c]+=1
                #     else:
                #         app_class_dict[a_c]=1
                a_c_str=app+':'+str(price)+'|'
            else:
                continue
            result+=a_c_str
        if result.split('\t')[1]=='':
            continue
        else:
            f2.write(result[:-1]+'\n')
    f1.close()
    f2.close()

def get_user_payment(inputFile, outputFile):
    f1=codecs.open(inputFile, 'r', 'utf-8')
    f2=codecs.open(outputFile, 'w', 'utf-8')
    for lines in f1.readlines():
        name, app_str=lines.strip('\r\n').split('\t')
        app_num=0
        app_sum_price=0.0
        for a_info in app_str.split('|'):
            try:
                app_name, price= a_info.split(':')
            except:
                continue
            app_num+=1
            app_sum_price+=float(price)
        if app_num==0:
            continue
        app_avg_price=app_sum_price/float(app_num)
        f2.write(name+'\t'+'app_number:'+str(app_num)+'\t'+'app_average_price:'+str(app_avg_price)+'\n')
    f1.close()
    f2.close()

def get_app_dist(inputFile, outputFile):
    app_dist_dict={}

    f1=codecs.open(inputFile, 'r', 'utf-8')
    f2=codecs.open(outputFile, 'w', 'utf-8')
    for lines in f1.readlines():
        name, app_str=lines.strip('\r\n').split('\t')
        for a_info in app_str.split('|'):
            try:
                app_name, price= a_info.split(':')
            except:
                continue
            if app_dist_dict.has_key(app_name):
                app_dist_dict[app_name]+=1
            else:
                app_dist_dict[app_name]=1

    for key, value in app_dist_dict.items():
        f2.write(key+'\t'+'users:'+str(value)+'\n')
    f1.close()
    f2.close()

def get_2fee_package_set(filename):
    '''
        get the set of 2.3w fee packages
        get_2fee_package_set('2.3wfee_package.csv')
    '''
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

def user_fee_app(input_file, output_file):
    '''
        The imfomation of each user who installed fee packages
        user_fee_app('data_0831/user_install_list','data_0831/user_fee_app')
    '''
    global app_pay_dict

    f1=codecs.open(input_file,'r','utf-8')
    f2=codecs.open(output_file,'w','utf-8')
    for lines in f1.readlines():
        idfa, package_str=lines.strip('\r\n').split('\t')
        result=idfa+'\t'
        for app in package_str.split('|'):
            if not app_pay_dict.has_key(app):
                continue
            result+=app+':'+str(app_pay_dict[app])+'|'
        if result.split('\t')[1]!='':
            f2.write(result[:-1]+'\n')
    f1.close()
    f2.close()

def get_app_num(input_file, output_file):
    '''
        The number of each app installed
        get_app_num('data_0831/user_install_list','data_0831/app_num')
    '''
    app_num_dict={}

    f1=codecs.open(input_file,'r','utf-8')
    f2=codecs.open(output_file,'w','utf-8')
    for lines in f1.readlines():
        idfa, package_str=lines.strip('\r\n').split('\t')
        for app in package_str.split('|'):
            if app_num_dict.has_key(app):
                app_num_dict[app]+=1
            else:
                app_num_dict[app]=1
    f1.close()

    app_num_list=sorted(app_num_dict.items(), key=lambda x:x[1], reverse=True)
    # import pdb;pdb.set_trace()
    user_num=float(os.popen('wc -l data_0831/user_install_list').read().strip('\r\n').split(' ')[0])
    for tuples in app_num_list:
        ratio='%.4f'%(tuples[1]/user_num)
        f2.write(tuples[0]+':'+str(tuples[1])+':'+ratio+'\n')
    f2.close()

def get_fee_appnum(input_file, output_file):
    global app_pay_dict
    app_num_dict={}

    f1=codecs.open(input_file,'r','utf-8')
    f2=codecs.open(output_file,'w','utf-8')
    for lines in f1.readlines():
        idfa, package_str=lines.strip('\r\n').split('\t')
        for app in package_str.split('|'):
            if app not in app_pay_dict:
                continue
            if app_num_dict.has_key(app):
                app_num_dict[app]+=1
            else:
                app_num_dict[app]=1
    f1.close()

    app_num_list=sorted(app_num_dict.items(), key=lambda x:x[1], reverse=True)
    for tuples in app_num_list:
        f2.write(tuples[0]+':'+str(tuples[1])+'\n')
    f2.close()

if __name__=='__main__':
    '''
        user_package: the install list of each user in the idfa_set
        packages: the set of packages
    '''
    # get_package_set('user_package', 'packages')
    # get_json_info()
    # get_app_price('packages_json', 'packages_price')
    # get_user_info('user_packages', 'user_packages_info')
    # get_user_payment('user_packages_info', 'user_payment')
    # get_app_dist('user_packages_info', 'app_distribute')

    # modified on 20150831
    get_2fee_package_set('2.3wfee_package.csv')
    user_fee_app('data_0831/user_install_list','data_0831/user_fee_app')
    get_app_num('data_0831/user_install_list','data_0831/app_num')
    get_fee_appnum('data_0831/user_install_list','data_0831/fee_app_num')