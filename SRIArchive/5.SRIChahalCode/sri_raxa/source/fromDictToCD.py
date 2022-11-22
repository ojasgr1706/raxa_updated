# --------------------------------------------------------------------------------
# SPADE - Support for Provenance Auditing in Distributed Environments.
# Copyright (C) 2015 SRI International
# This program is free software: you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
# --------------------------------------------------------------------------------


##will take the dictionnary {grade, [words]} to create a dataset summing up the results for each patient


from makeTodo import *
from toolsTodoSummedUp import *
from onTodo import *
from dictionnary import *
import re
import pandas as pd
import os.path


pathResult = "resultDir/resDic.csv"

todo = add_concept_form_db()
final = todo[['Reg no.','patient_id']].drop_duplicates() 
final = final[~final['Reg no.'].isnull()] 
final =addCCI(final, "/Users/coulson/rxc/sri_raxa/source/splitting by POD/Anonymized_CCI-CD_without_outliers.csv")
dictToCD = pd.DataFrame([('0',0),('I',1), ('II',2), ('IIIa',3), ('IIIb',4), ('IV a',5), ('IV b',6), ('IVa',5), ('IVb',6), ('V',7)])
dictToCD.columns = ['CD','CDN']

final = pd.merge(final, dictToCD, how='left')
final['CD'][final['CD']=='IV a ']= 'IV a'
final['CD'][final['CD']=='IV a']= 'IVa'
final['CD'][final['CD']=='IV b']= 'IVb'
final['CD'][final['CD']=='V ']= 'V'   
final['ind'] = final['CDN']>3
onId = ['160632', '170250000', '160250', '33334452']

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
totalRes =[]
for pat in list(np.unique(final['patient_id'])):
    maxGrade='0'
    maxPOD =0
    todoSel = todo[(todo['patient_id']==pat) & (todo['concept_id'].apply(lambda x: x in onId)) & (str(todo['value_text']) != 'nan')]
    todoSel = todoSel[['obs_datetime','value_text', 'concept_id']]
    todoForPOD = todoSel[todoSel['value_text'].apply(lambda x : 'pod' in x.lower())]
    if(todoSel.shape[0]>0):
        ### To find surgery date
        dates=[]
        ##last op
        date =None
        for i in range(todoForPOD.shape[0]):

            if(re.search('\d+', todoForPOD['value_text'].values[i]) != None):
                nb = int(re.search('\d+', todoForPOD['value_text'].values[i]).group())
                if(nb>maxPOD):
                    maxPOD=nb
                date = todoForPOD['obs_datetime'].values[i]
                date =date + datetime.timedelta(days=-nb)
                dates.append(date)
                
        if(date==None):
            print('Patient '+pat+' without surgery date')
        
        res= []  
        ##To find words and grade
        for grade,listWord in listDic.items():
            listConcept = dicConcept[grade]
            for i in listConcept:
                if(i in list(todoSel['concept_id'])):
                    res = res+[(grade, i, None)]
                    maxGrade = grade
            todoSel = todoSel[todoSel['value_text'].apply(lambda x: 'pod' not in x.lower())]
            for i in listWord:
                ##for each word
                 if(type(i)==str):
                     if('icu' in i):
                         resTmp =list(todoSel['obs_datetime'][todoSel['value_text'].apply(lambda x: i in x.lower() and 'obs' not in x.lower() and 'poicu' not in x.lower())].values)
                     else:
                         resTmp =list(todoSel['obs_datetime'][todoSel['value_text'].apply(lambda x: i in x.lower())].values)
                    
                ##for each list
                 elif(type(i)==list):
                     somme = todoSel['value_text'].apply(lambda x: i[0] in x.lower())
                     for j in i[1:]:
                         somme = np.logical_and(somme, todoSel['value_text'].apply(lambda x: j in x.lower()))
                     resTmp =list(todoSel['obs_datetime'][somme].values)
                 deltas =[]
                 for dateToDo in resTmp:
                     if(dateToDo != None):  # and between1_30(dates, date) != None):
                         deltas.append(between1_30(dates, dateToDo))
                     else:
                         deltas.append(dateToDo)
                 ## TODO   ATTENTION AUX DATES 
                 if(len(deltas)>0):
                     res = res+[(grade, i, unique(deltas))]
                     maxGrade = grade
                     
        totalRes.append([pat,maxGrade, str(date), to_str(dates), len(list(np.unique(dates))), maxPOD, str(res)])

totalRes = pd.DataFrame(totalRes)
totalRes.columns =['patient_id', 'maxCD', 'surgeryDate', 'allSurgeriesDates', 'countSurgeries', 'maxPOD', 'words']
finCSV = pd.merge(final,totalRes, how='left')
dictToCDMax =dictToCD
dictToCDMax.columns = ['maxCD','maxCDN']
finCSV = pd.merge(finCSV, dictToCDMax, how='left')
finCSV['diffCDN'] = finCSV['maxCDN'] - finCSV['CDN']
finCSV[['Reg no.','patient_id','CD','CCI','maxCD','CDN','maxCDN','diffCDN','surgeryDate','allSurgeriesDates','countSurgeries','maxPOD','words']].to_csv(pathResult, index=False)

print(pathResult)
##check surgery date in order table