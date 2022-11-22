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

import re
import numpy as np
from collections import Counter
import datetime 
from toolsTodoSummedUp import *

#numerical values
numDic = {'5089':'weight',
             '5090':'Height',
             '1132':'SERUM_SODIUM',
             '1133':'SERUM_POTASSIUM',
             '790':'SERUM_CREATININE',
             '159497':'SERUM_CALCIUM',
             '21':'hemoglobin',
             '33334014':'Phosphorous_test',
             '9': 'BLOOD SUGAR',
             '678':'WBC',
             '857':'UREA',
             '848':'SERUM_ALBUMIN',
             '1297':'DIRECT_BILIRUBIN',
             '654':'SGPT',
             '655':'BILIRUBIN',
             '653':'SGOT',
             '717':'TOTAL_PROTEIN',
             '785':'ALKALINE_PHOSPHATASE',
             '159829':'SERUM_GGT',
             '33334012':	'prothrombin',
             '33334014': 'Phosphorous Test'
             }

kvList = ['pulse', 'bp', 'urine output', 'urine',
       'intake', 'temp', 'stoma', 'rt', 'spo2']

# Boolean values (may have 3 different values)
boolDic = {
        '33334432':	'Does Patient Have A Difficult Airway/Aspiration Risk?', #YES NO
        '33334433':	'Does Patient Have A Risk of 500ml blood loss(7ml/kg in children)?',#YES NO
        '33334440':	'Has antibiotic prophylaxis been given within the last 60 minutes?',#YES NOT APPLICABLE
        '33334442':	'Has DVT prophylaxis been administered',#Yes not required
        '33334448':	'Is essential imaging displayed', # yes not required
        '33334434' : 'Has the imaging been discussed with radiologist preoperatively', #yes no not required
        '1449' : 'Gestational diabetes'
        }


def extract_value_from_valuetext(todo, last, funToMerge =lambda x:x):
    res = []
    col = []
    enc = 0  # to run through the list of last
    
    # To correct the location of numerical values
    todo['value_numeric'][todo['concept_id']=='33334012'] = todo['value_text'][todo['concept_id']=='33334012']
    for k,v in numDic.items():
        col.append(v)
        res.append(funToMerge(bindAll(last[enc],getNum(todo, k, list))))
        enc=enc+1
        
     # Boolean
    for k,v in boolDic.items():
        col.append(v)
        res.append(funToMerge(bindAll(last[enc], getBool(todo, k, list))))
        enc=enc+1
     
    # KVExtracted
    for l in kvList:
        col.append(l)
        res.append(funToMerge(bindAll(last[enc], getKV(todo, l, list))))
        enc=enc+1
        
    # PLT, concept_id=1026
    res.append(funToMerge(bindAll(last[enc], getPLT(todo))))
    enc=enc+1

    # newNLP: 1: 'words',2: 'maxCD', 3: 'maxCDN'

    newNLP= NLPperPatientSumedUp(todo)
    tmpRes=[]
    
    tmpRes.append(bindAll(last[enc], newNLP[0]))
    enc=enc+1
    # tmpRes.append(myMax(last[enc], newNLP[1]))
    # enc=enc+1
    # tmpRes.append(myMax(last[enc], newNLP[2]))
    # enc=enc+1
    # tmpRes.append(newNLP[1])
    # tmpRes.append(newNLP[2])
    res = res+tmpRes
    return res
