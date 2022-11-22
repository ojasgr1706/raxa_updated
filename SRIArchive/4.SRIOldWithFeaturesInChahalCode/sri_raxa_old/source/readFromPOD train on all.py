#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 24 10:05:07 2017

@author: study
"""

import pandas as pd
from csv2arff import *
from classifier import *
from sklearn.cross_validation import StratifiedShuffleSplit
from sklearn.preprocessing import Imputer
import matplotlib.pyplot as plt
import numpy as np


pathToPOD = 'resultDir/'
version = '10'
n_iter=15

listCol = ['weight',
         'Height',
         'SERUM_SODIUM',
         'SERUM_POTASSIUM',
         'SERUM_CREATININE',
         'SERUM_CALCIUM',
         'hemoglobin',
         'Phosphorous Test',
         'WBC',
         'UREA',
         'SERUM_ALBUMIN',
         'DIRECT_BILIRUBIN',
         'SGPT',
         'BILIRUBIN',
         'SGOT',
         'TOTAL_PROTEIN',
         'ALKALINE_PHOSPHATASE',
         'SERUM_GGT',
         'prothrombin',
         'Does Patient Have A Difficult Airway/Aspiration Risk?',
         'Does Patient Have A Risk of 500ml blood loss(7ml/kg in children)?',
         'Has antibiotic prophylaxis been given within the last 60 minutes?',
         'Has DVT prophylaxis been administered',
         'Is essential imaging displayed',
         'Has the imaging been discussed with radiologist preoperatively',
         
         'PLT',
         'words',
         'maxCD',
         'maxCDN',
         'newCD',
         'newCDN']

listNum = ['weight',
         'Height',
         'SERUM_SODIUM',
         'SERUM_POTASSIUM',
         'SERUM_CREATININE',
         'SERUM_CALCIUM',
         'hemoglobin',
         'Phosphorous Test',
         'WBC',
         'UREA',
         'SERUM_ALBUMIN',
         'DIRECT_BILIRUBIN',
         'SGPT',
         'BILIRUBIN',
         'SGOT',
         'TOTAL_PROTEIN',
         'ALKALINE_PHOSPHATASE',
         'SERUM_GGT',
         'prothrombin']

listBool = ['Does Patient Have A Difficult Airway/Aspiration Risk?',
     'Does Patient Have A Risk of 500ml blood loss(7ml/kg in children)?',
     'Has antibiotic prophylaxis been given within the last 60 minutes?',
     'Has DVT prophylaxis been administered',
     'Is essential imaging displayed',
     'Has the imaging been discussed with radiologist preoperatively']

listFeatures = [#'newCDN',
                'POD']
listExtracted = ['pulse', #'bp', 
                 'urine output', 'urine', 'intake', 'temp', 'stoma', 'rt', 'spo2']


listWords = ['words']

def funOnNum(x):
    if(x==None):
        return [None,None]
    res=[]
    for dat,n in x:
        if(type(n)==float or type(n) == int or n.isdigit()):
            res.append(float(n))
    return [np.mean(res), np.std(res)]
    

def funOnBool(x):
    if(x==None):
        return None
    res=False
    for dat,n in x:
        res =res or n
    return res
    


###################################
final = pd.read_pickle(pathToPOD+version+'.pckl')

for name in listNum+listExtracted:
    res = final[name].apply(funOnNum)
    final[name], final[name+'-sd'] = res.apply(lambda x: x[0]), res.apply(lambda x: x[1])

for name in listBool:
    final[name] = final[name].apply(funOnBool)

#
#toTest = final[listNum+listBool+['patient_id', 'CD', 'CDN', 'maxCD', 'maxCDN', 'newCD', 'newCDN', 'ind']]
#toTest.to_csv(pathToPOD+'final'+version+'.csv', index=False, sep='\t')
#
#csv2arff(pathToPOD+'final'+version+'.csv')#, pathToPOD+'final'+version+'.arff')
#    

final['POD'][final['POD'] =='max'] = 9999999
final['POD'] = final['POD'].astype(int)
#final['<IIIa'] = final[['I', 'II']].apply(sum, axis=1)
#final['>=IIIa'] = final[['IIIa', 'IIIb', 'IVa','IVb','V']].apply(sum, axis=1)
X_df_all = final[listNum+listBool+listFeatures+listExtracted+['patient_id', 'ind']] #'maxCDN', 


res ={}
accTot = []
accpostot=[]
accFPtot=[]
accFNtot=[]
maxPOD = 50
minPOD = 0
toPlotx = []
resdf = np.zeros((1,30))
 
prob = [0.5,0.5]
                
for pod in list(np.unique(final['POD']))[:-20]:
    print('------- POD: '+str(pod)+'-------')
    indexes = final['POD']<=pod
    ave=[]
    acc=[]
    accpos=[]
    accfn=[]
    accfp=[]
    for kk in range(n_iter):
        X_todo = X_df_all[indexes]               
        X_rest = X_df_all[~indexes]               
        X_t = X_df_all[X_df_all['ind']==True]
        X_f = X_df_all[X_df_all['ind']==False]
        
        indexesTodo = np.random.choice(2,X_t.shape[0], p=prob)==0
        X_df = X_t[indexesTodo]
        xTest_df = X_t[~indexesTodo]
        indexesTodo = np.random.choice(2,X_f.shape[0], p=prob)==0
        X_df = pd.concat([X_df,X_f[indexesTodo]])
        xTest_df = pd.concat([xTest_df,X_f[~indexesTodo]])
        X_df =  pd.concat([X_df, X_rest])   
        
        if(X_df.shape[0]/(X_df.shape[0] + xTest_df.shape[0]) > 0.7):
            pr = 0.7*(X_df.shape[0] + xTest_df.shape[0])/X_df.shape[0]
            X_df = X_df[np.random.choice(2,X_df.shape[0], p=[pr,1-pr])==0]
        
        x = X_df.drop(['patient_id','ind'], 1).values          
        y = X_df['ind'].values               
        X = xTest_df.drop(['patient_id','ind'], 1).values   
        Y = xTest_df['ind'].values 
        
        imp = Imputer(missing_values='NaN', strategy='median', axis=0)
        imp = imp.fit(x)
        x = imp.transform(x)
        clf = MyClf()
        clf.train(x,y)
        
        X = imp.transform(X)
        ave.append(clf.mse(X,Y))
        acc.append(clf.accuracy(X,Y))
        accpos.append(sum((clf.predict(X)[Y==1]>0.3)*1)/Y[Y==1].shape[0])
        accfp.append(sum((clf.predict(X)[Y==0]>0.3)*1)/Y[Y==1].shape[0])
        accfn.append(sum((clf.predict(X)[Y==1]<=0.3)*1)/Y[Y==1].shape[0])
    
    res[pod]=ave
    accTot.append(np.mean(acc))
    accpostot.append(np.mean(accpos))
    accFPtot.append(np.mean(accfp))
    accFNtot.append(np.mean(accfn))
    toPlotx.append(pod)
        
       # print(sumConf/(sum(sum(sumConf))))
resDF = pd.DataFrame(resdf)
resave =[]
ressd =[]
tout=[]
for i in res.values():
    resave.append(np.mean(i))   
    ressd.append(np.std(i)) 
    tout.append([np.mean(i), np.std(i)])    
       

def fairePlotAll(l, final, name=""):
    if(seuil==-1):
        plt.plot(list(np.unique(final['POD']))[:-6], l[:-6])
        plt.plot([1,2,7,14], [(l[i]) for i in [1,2,7,14]], 'ro')
    else:
        resx = []
        resy =[]
        c=0
        for i in list(np.unique(final['POD'])):
            if(i<=seuil):
                resx.append(i)
                resy.append(l[c])
            c=c+1
        plt.plot(resx,resy, label=name)
        plt.legend(loc=3, borderaxespad=0.)
        plt.plot([1,2,7,14], [(l[i]) for i in [1,2,7,14]], 'ro')

def fairePlot(x,y, name="", seuil=-1):
    if(seuil==-1):
        plt.plot(x[:-20], y[:-20], label=name)
    else:
        plt.plot(x, y, label=name)
    plt.legend(loc=1, borderaxespad=0.)
    #plt.plot([1,2,7,14], [(y[i]) for i in [1,2,7,14]], 'ro')
    

def showImportance(clf, X_df):
    res =pd.DataFrame(X_df.drop(['patient_id', 'ind'], 1).columns, clf.clf.feature_importances_).reset_index().sort_values('index', ascending=1)
    res[res['index']>0.01].plot.barh(x=0)
    
showImportance(clf, X_df_all)
plt.show()   
#
#fairePlotAll(accpostot, final, 100, name='discovery')
#fairePlotAll(accFNtot, final, 100, name='FN')
#fairePlotAll(accFPtot, final, 100, name='FP')


fairePlot(toPlotx, accpostot, name='discovery', seuil=0)
fairePlot(toPlotx, accFNtot, name='FN', seuil=0)
fairePlot(toPlotx, accFPtot, name='FP', seuil=0)


pd.DataFrame({'x':toPlotx, 'accPos':accpostot, 'FP':accFPtot, 'FN':accFNtot}).to_csv('/Users/coulson/Desktop/extratree.csv', index=False)