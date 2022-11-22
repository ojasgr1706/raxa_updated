#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 24 11:23:30 2017

@author: study
"""
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import GradientBoostingRegressor
class MyClf():

    def __init__(self):
        self.clf = RandomForestRegressor(n_estimators =200, n_jobs=-1, verbose=0, random_state=56)
        pass
    
    def train(self, X, y):
        self.clf.fit(X,y)
        
    def predict(self, X):
        return self.clf.predict(X)
    
    def accuracy(self, X,y):
        pred = self.predict(X)
        return accuracy_score(y,(pred>0.5)*1)
    
    def confusion_matrix(self, X,y):
        pred = self.predict(X)
        return confusion_matrix(y, (pred>0.5)*1)
    
    def mse(self, X,y):
        return mean_squared_error(y, self.predict(X))