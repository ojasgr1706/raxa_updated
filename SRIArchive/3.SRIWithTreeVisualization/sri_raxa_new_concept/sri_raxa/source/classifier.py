#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 24 11:23:30 2017

@author: study
"""
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import GradientBoostingRegressor
import pandas as pd
from sklearn.tree import export_graphviz
import pydot
import os

currentDirectoryPath = os.path.dirname(os.path.abspath(__file__))
graphDirectory = currentDirectoryPath + '/visualization/'


class MyClf():
    """
        n_estimators: This is the number of trees you want to build before taking the maximum voting or averages of predictions.
        n_jobs: tells the engine how many processors is it allowed to use. A value of “-1”
        means there is no restriction whereas a value of “1” means it can only use one processor.
     """

    def __init__(self):
        # self.clf = RandomForestClassifier(n_estimators=250, criterion='gini', n_jobs=-1, class_weight={0: 1, 1: 50}, verbose=0, max_depth = 10,
        #                                   random_state=56)
        self.clf = RandomForestClassifier(n_estimators=250, criterion='entropy', n_jobs=-1, class_weight={0: 1, 1: 50},
                                          verbose=0,
                                          random_state=56)
        # self.clf = GradientBoostingRegressor(n_estimators=250, random_state=56)
        pass

    def train(self, X, y):
        self.clf.fit(X, y)

    def predict(self, X):
        return self.clf.predict(X)

    def accuracy(self, X, y):
        pred = self.predict(X)
        return accuracy_score(y, (pred > 0.5) * 1)

    def confusion_matrix(self, X, y):
        pred = self.predict(X)
        return confusion_matrix(y, (pred > 0.5) * 1)

    def mse(self, X, y):
        return mean_squared_error(y, self.predict(X))

    def features_importance(self, X_train):
        feature_importances = pd.DataFrame(self.feature_importances_,
                                           index=X_train.columns,
                                           columns=['importance']).sort_values('importance', ascending=False)
        return feature_importances

    def export_decision_tree(self, feature_list, counter):
        fileName_dot = graphDirectory + 'tree' + str(counter) + '.dot'
        fileName_png = graphDirectory + 'tree' + str(counter) + '.png'

        tree = self.clf.estimators_[5]
        # Export the image to a dot file
        export_graphviz(tree, out_file=(fileName_dot), feature_names=feature_list, rounded=True, precision=1)
        # Use dot file to create a graph
        (graph,) = pydot.graph_from_dot_file(fileName_dot)
        # Write graph to a png file
        graph.write_png(fileName_png)
        print('Saved the Tree png')