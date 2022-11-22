#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 11 13:13:06 2017

@author: study
"""

import pandas as pd
import numpy as np

def csv2arffRun(filein = '/Users/coulson/rxc/sri_raxa/source/splitting by POD/resultDir/8.csv',
                fileout = '/Users/coulson/rxc/todo.arff'):
    
    df = pd.read_csv(filein, sep='\t')
    
    df['CD']=df['CD'].apply(lambda x : x.strip())
    #df['CD'][df['CD']=='IIIb'] = 'IIIa'
    #df['CD'][df['CD']=='IV b'] = 'IV a'
    
    
    out = open(fileout, 'w')
    out.write('@RELATION data\n\n')
    t = df.dtypes
    i=0
    dic = {'int64':'NUMERIC',
           'float64':'NUMERIC'
           }
    for c in df.columns:
        out.write('@ATTRIBUTE '+str(c).replace(',','_').replace('\'','_').replace(' ','_').replace(' ','_')+' ')
        
        if(str(t[i]) == 'object'):
            out.write('{'+','.join([str(k.replace(' ','_')) for k in list(np.sort(list(df[c].unique())))])+'}\n')
        else:
            out.write(dic[str(t[i])]+'\n')
        i=i+1
    
    
    out.write('\n@DATA\n')
    
    
    
    df.fillna('?').to_csv('/tmp/touse.csv', index=False)
    
    f = open('/tmp/touse.csv', 'r')
    for l in f.readlines()[1:] :
        out.write(l.replace(' ','_'))
    f.close()
    out.close()
