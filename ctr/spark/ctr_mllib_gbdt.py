#!/usr/bin/python
#-*- coding:utf8 -*-
"""
Gradient boosted Trees classification and regression using MLlib.
"""
from __future__ import print_function

import sys

from pyspark.context import SparkContext
from pyspark.mllib.tree import GradientBoostedTrees
from pyspark.mllib.util import MLUtils


def testClassification(trainingData, testData):
    # Train a GradientBoostedTrees model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    model = GradientBoostedTrees.trainClassifier(trainingData, categoricalFeaturesInfo={},
                                                 numIterations=30, maxDepth=4)
    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda v_p: v_p[0] != v_p[1]).count() \
        / float(testData.count())
    print('Test Error = ' + str(testErr))
    print('Learned classification ensemble model:')
    print(model.toDebugString())


def testRegression(trainingData, testData, model_path):
    # Train a GradientBoostedTrees model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    model = GradientBoostedTrees.trainRegressor(trainingData, categoricalFeaturesInfo={},
                                                numIterations=3, maxDepth=4)
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testMSE = labelsAndPredictions.map(lambda vp: (vp[0] - vp[1]) * (vp[0] - vp[1])).sum() \
        / float(testData.count())
    print('Test Mean Squared Error = ' + str(testMSE))
    print('Learned regression GBT model:')
    print(model.toDebugString())
    model.save(sc, model_path)

if __name__ == "__main__":
    # if len(sys.argv) > 1:
    #     print("Usage: gradient_boosted_trees", file=sys.stderr)
    #     exit(1)
    sc = SparkContext(appName="PythonGradientBoostedTrees")

    feature_file=sys.argv[1]
    model_path=sys.argv[2]

    data = MLUtils.loadLibSVMFile(sc, feature_file)
    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # print('\nRunning example of classification using GradientBoostedTrees\n')
    # testClassification(trainingData, testData)

    print('\nRunning example of regression using GradientBoostedTrees\n')
    testRegression(trainingData, testData, model_path)

    sc.stop()
