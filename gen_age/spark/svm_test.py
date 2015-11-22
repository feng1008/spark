#!/usr/bin/python
#-*- coding:utf8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint
import os

def parse(lines):
    values = [float(x) for x in lines.strip('\r\n').split(' ')]
    return LabeledPoint(values[0], values[1:])

def main(sc):
    train_data='/usr/local/spark/data/mllib/sample_svm_data.txt'
    data=sc.textFile(train_data).map(parse)
    
    if os.path.exists('model'):
        model=SVMModel.load(sc, 'model')
    else:
        model=SVMWithSGD.train(data, iterations=100)
        model.save(sc, 'model')

    labelsAndPreds=data.map(lambda p: (p.label, model.predict(p.features)))

    # trainErr=labelsAndPreds.filter(lambda (v, p): v != p).count() / float(data.count())
    # print('Training Error ='  + str(trainErr))

    labelsAndPreds.map(lambda x:str(x[0])+'\t'+str(x[1])).saveAsTextFile('labelsAndPreds')

if __name__=="__main__":
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    main(sc)