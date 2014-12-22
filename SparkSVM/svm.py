'''
Ridge Regression and SVM Regression 
using Spark's MLlib

Author: John C. Terzis
Date: 12/13/14
'''
import os
import sys 
import numpy as np
import pandas as pd

from sklearn import preprocessing
from sklearn import cross_validation
from sklearn.preprocessing import normalize
from sklearn.externals import joblib
from sklearn.metrics import r2_score

import matplotlib.pyplot
from pyspark import SparkContext #MUST RUN FROM pyspark python shell if interactive
from pyspark.mllib.regression import LabeledPoint, \
RidgeRegressionWithSGD, LassoWithSGD
#from pyspark.mllib.feature import StandardScaler
from pyspark.mllib.linalg import Vectors

import constants as con




def postProcess(file, vol):
    '''
    Extract column header, scale using scikitlearn and 
    prune columns for consumption by a regressor that 
    accepts text file of form  y,x1,x2,x3.....
    
    file = raw text file with column structure as per constants.py
    vol = what vol to predict ['1','7','20','40'...]
    '''
    if con.HDF == False:
        df = pd.read_csv(con.DATA_PATH + con.FILE_NM)
        #column name parsing 
        df.rename(columns = lambda x: x.replace(' ','')[1:-1], inplace=True)
        df_keep = df.ix[:,con.OUTPUT_COL_KEEP + con.INPUT_COL_KEEP]
        f = lambda x: np.nan if 'inf' in str(x) else float(x)   
        df_keep = df_keep.dropna()
        out_col = 'SYMBOL_+' + str(vol) + 'D'
        if out_col in df_keep.columns.tolist():
            df_keep = df_keep.ix[:,[out_col] + con.INPUT_COL_KEEP]
        #scale columnwise use std scaler
        input = np.array(df_keep[con.INPUT_COL_KEEP])
        #target = np.array(df_keep[con.INPUT_COL_KEEP]).flatten()
        #scale input such that each feature has zero mean and unit var
        scaler = preprocessing.StandardScaler().fit(input)
        input_normal = scaler.transform(input)
        
        df_keep_std = pd.DataFrame(input_normal)
        df_keep_std['y'] = df_keep[out_col] #add output col
        df_keep_std = df_keep_std[['y'] + df_keep_std.columns.tolist()[:-1]]
        target = np.array(df_keep_std['y']).flatten()
        
        #split into training and test data since pyspark does not have this util
        x_train, x_test, y_train, y_test = cross_validation.train_test_split(
                                             input_normal, target, test_size = .20, 
                                             random_state=42)
        df_train = pd.DataFrame(x_train, columns = con.INPUT_COL_KEEP)
        df_train['y'] = pd.Series(y_train)
        df_train = df_train.ix[ : , ['y'] + con.INPUT_COL_KEEP].fillna(0)
        df_train.to_csv(os.path.abspath(os.curdir) + '/data/' + con.TRAIN_FN,
                            sep=",", header=False, index=False)
        
        df_test = pd.DataFrame(x_test, columns = con.INPUT_COL_KEEP)
        df_test['y'] = pd.Series(y_test)
        df_test= df_test.ix[ : , ['y'] + con.INPUT_COL_KEEP].fillna(0)
        df_test.to_csv(os.path.abspath(os.curdir) + '/data/' + con.TEST_FN,
                            sep=",", header=False, index=False)
        
        #write to text for consumption by mllib
        df_keep_std.to_csv(os.path.abspath(os.curdir) + '/data/' + con.PROCESSED_FN,
                            sep=",", header=False,index=False)
    else: #its an hdf file
        pass


def parsePoint(line):
    '''
    Load and parse data line by line
    '''
    values = [float(s) for s in line.split(',')[1:]]
    if values[0] == -1:   # Convert -1 labels to 0 for MLlib
        values[0] = 0
    return LabeledPoint(values[0], values[1:])
    
    
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: ridge_regression <text_file> <initial_weight> <vol>"
        print >> sys.stderr, "Using Defaults!"
        y_predict = []
        
        if con.HDF == False:
            sc = SparkContext(appName="VolEstimation")
            
        #post-process input in pandas
        postProcess(con.DATA_PATH + con.FILE_NM, vol=7)
        
        xy_test_points = pd.read_csv(os.path.abspath(os.curdir) + 
                             '/data/' + con.TEST_FN)
        x_test_points = xy_test_points.ix[:,1:11].values.tolist() #omit y output col
        y_test_points = xy_test_points.ix[:,0].values.tolist()
        
        xy_train_points = sc.textFile(os.path.abspath(os.curdir) + 
                             '/data/' + con.TRAIN_FN).map(parsePoint)
                             
        model = RidgeRegressionWithSGD.train(xy_train_points, iterations=5000)
        
        for x in x_test_points:
            y_predict.append(model.predict(x))
        r2_knn = r2_score(y_test_points,y_predict)
        
        print "Final Out of Sample R^2 of Regression" + str(r2_knn)
        print "Final weights: " + str(model.weights)
        print "Final intercept: " + str(model.intercept)
        
        #kill Spark context gracefully
        sc.stop()
    
    
    