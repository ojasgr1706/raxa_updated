#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 22 15:20:18 2017

@author: study
"""

import pandas as pd
import numpy as np
from dictionnary import *
import re
import datetime

dictToCD = pd.DataFrame([('0',0),('I',1), ('II',2), ('IIIa',3), ('IIIb',4), ('IV a',5), ('IV b',6), ('IVa',5), ('IVb',6), ('V',7)])
dictToCD.columns = ['CD','CDN']

##dictionnary for CD
listDic = getDictionnary()
    
dicConcept = {
        'I': ['33334442' # 'Has DVT prophylaxis been administered'
              ],
        'II': ['33334440' #'Has antibiotic prophylaxis been given within the last 60 minutes?'
        ],
        'IIIa': [],
        'IIIb': [],
        'IVa': [],
        'IVb': [],
        'V': [],
             }
##recognize the CD from the onId concept_ids
def NLPperPatientSumedUp(todoPat, onId = ['160632', '170250000']):
    totalRes =[]
    maxGrade='0'
    todoSel = todoPat[(todoPat['concept_id'].apply(lambda x: x in onId)) & (str(todoPat['value_text']) != 'nan')]
    todoSel = todoSel[['obs_datetime','value_text', 'concept_id']]
    
    if(todoSel.shape[0]>0):
        res= []  
        ##To find words and grade
        for grade,listWord in listDic.items():
            listConcept = dicConcept[grade]
            for i in listConcept:
                if(i in list(todoSel['concept_id'])):
                    res = res+[(grade, i, None)]
                    maxGrade = grade
            
            for i in listWord:
                resTmp=[]
                ##for each word
                if(type(i)==str):
                    resTmp =list(todoSel['obs_datetime'][todoSel['value_text'].apply(lambda x: i in x.lower())].values)
                ##for each list
                elif(type(i)==list):
                     somme = todoSel['value_text'].apply(lambda x: i[0] in x.lower())
                     for j in i[1:]:
                         somme = np.logical_and(somme, todoSel['value_text'].apply(lambda x: j in x.lower()))
                     resTmp =list(todoSel['obs_datetime'][somme].values)

                 ## TODO   ATTENTION AUX DATES 
                if(len(resTmp)>0):
                     res = res+[(grade, i, unique(resTmp))]
                     maxGrade = grade
                     
        totalRes.append([maxGrade, res])

    totalRes = pd.DataFrame(totalRes)
    if(totalRes.empty == True):
        return [None,None,None]
    totalRes.columns =['maxCD', 'words']
    dictToCDMax =dictToCD
    dictToCDMax.columns = ['maxCD','maxCDN']
    finCSV = pd.merge(totalRes, dictToCDMax, how='left')
    return finCSV[['words','maxCD','maxCDN']].values.tolist()[0] #TODO: PKKKKK ??????
    
#merge list with any type of field
def bindAll(last, enc):
    res = []
    if(last == None):
        res = enc
    elif(enc==None):
        res = last
    elif(type(last)!=list):
        res = [last,enc]
    elif(type(enc)!=list):
        res = [last,enc]
    else:
        res = last + enc
    return unique(res)

#get the closest surgery in the past
def getCloseSurgery(dat, listSurgeryDate):
    res = datetime.date.min
    minpos = (dat-res).days
    for i in listSurgeryDate:
        if((dat-i).days <= minpos and (dat-i).days>=0):
            minpos = (dat-i).days
            res = i
    return res

#get all the surgeries and their POD to [dat]
def getAllSurgery(dat, listSurgeryDate):
    res = []
    for i in listSurgeryDate:
        res.append((i, (dat-i).days))
    return res

#get the minimum of PODs (can be negative)
def getMinPOD(dat, listSurgeryDate):
    res = datetime.date.min
    minpos = (dat-res).days
    for i in listSurgeryDate:
        if((dat-i).days <= minpos):
            minpos = (dat-i).days
            res = i
    return minpos

#get the PODs only if it is between 0 and 30
def between1_30(dates, date):
    d=100000
    for i in dates:
        d =min(max((date-i).days,0), d)
    return (str(date),d)


#get the max including comparison with None values
def myMax(a,b):
    if a==None:
        return b
    if b==None:
        return a
    return max(a,b)

#get unique without using numpy : allow list of list
def unique(l):
    if(l == None):
        return None
    res =[]
    for i in l:
        if i not in res:
            res.append(i)
    return res

#equivalent of str() for list of everything
def to_str(listDate):
    return unique([str(x) for x in listDate])

#check for each row of df if all elements of list(l) are in df['value_text']
def allIn(df, l):
    df['resultDir'] = 0
    for i in l:
        df['resultDir'] = df['resultDir']+(df['value_text'].apply(lambda x: i in x.lower()))*1
    res= df['resultDir']==len(l)
    del df['resultDir']
    return res

#calculate the mean for of personnalized type
def Rmean(x):
    if(str(x)=='nan'):
        return np.nan
    res =[]
    for _,i in x:
        res.append(int(i))
    return float(np.mean(res))

#get values from concept_id = 1026
def getPLT(todo):
    tmp = todo[['patient_id', 'value_text', 'obs_datetime']][todo['concept_id'] == '1026']
    if(tmp.empty==False):
        tmp['PLT'] = tmp['value_text'].apply(lambda x : re.search(r'\d+',x).group())
        tmp['tot'] = tmp.apply(lambda x : (x[2], x[3]), axis=1)
        tmp = tmp[['patient_id', 'tot']].groupby('patient_id')['tot'].apply(list).reset_index()
        return tmp['tot'].values.tolist()[0]#TODO: CLEAN
    else:
        return None

#get all values from booleans fields
def getBool(todo, concept_id, fun):
    tmp = todo[['patient_id', 'value_text', 'obs_datetime']][todo['concept_id'] == concept_id]
    if(tmp.empty==False):
        tmp['value_text'] = tmp['value_text'].apply(lambda x: 'yes' in x.lower()).astype(bool)
        tmp['tot'] = tmp.apply(lambda x : (x[2], x[1]), axis=1)
        tmp = tmp[['patient_id', 'tot']].groupby('patient_id')['tot'].apply(fun).reset_index()
        return tmp['tot'].values.tolist()[0]#TODO: CLEAN
    else:
        return None
    
#get all values from kv etractor fields
def getKV(todo, name, fun):
    tmp = todo[['patient_id', name, 'obs_datetime']][~todo[name].isnull()]
    if not tmp.empty:
        tmp['tot'] = tmp.apply(lambda x : (x[2], x[1]), axis=1)
        tmp = tmp[['patient_id', 'tot']].groupby('patient_id')['tot'].apply(fun).reset_index()
        return tmp['tot'].values.tolist()[0]#TODO: CLEAN
    else:
        return None
0
#get all values from numerical fields
def getNum(todo, concept_id, fun):
    tmp = todo[['patient_id', 'value_numeric', 'obs_datetime']][todo['concept_id'] == concept_id]
    if(tmp.empty==False):
        tmp['value_numeric'] = tmp['value_numeric'].astype(float)
        tmp['tot'] = tmp.apply(lambda x : (x[2], x[1]), axis=1)
        tmp = tmp[['patient_id', 'tot']].groupby('patient_id')['tot'].apply(fun).reset_index()
        return tmp['tot'].values.tolist()[0]#TODO: CLEAN
    else:
        return None

#merge with the CCI file
def addCCI(todo, pathToCCIs):
    r = pd.read_csv(pathToCCIs, sep=',')
    r = r[['Reg no.', 'CD', 'CCI']]
    r = r[~r['Reg no.'].isnull()]
    r['Reg no.'] = r['Reg no.'].astype(int)
    todo['Reg no.'] = todo['Reg no.'].astype(int)
    #r[~r['Reg no.'].str.contains('City')]
    return pd.merge(todo, r, how='left')






#merge merge with the results from cursor and give it the names and type, possibly drop nas
def mergeNtype(todo, cursor, lname, typ=int, dropna = False):
    res = pd.DataFrame(cursor.fetchall())
    res.columns = lname
    res =pd.merge(todo, res, how='left')
    if(dropna):
        res = res.dropna()
    if(typ != None):
        res[lname[-1]] = res[lname[-1]].astype(typ)
    return res
