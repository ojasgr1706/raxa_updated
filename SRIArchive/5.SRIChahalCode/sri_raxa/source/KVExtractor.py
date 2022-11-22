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

## NEED TO BE IMPROVED !!!!!!!!!!

def runKVExtractor(directoryData, cursor, todo): 
    
    query = ("SELECT obs_id, value_text, concept_id, obs_datetime, patient_id FROM obs, encounter where obs.encounter_id = encounter.encounter_id and concept_id=160632 and patient_id in (" + 
             ','.join(list(np.unique(todo['patient_id'])))+")")

    cursor.execute(query)
    
    lrow = []
    for row in cursor: 
        if(type(row[1]) == str): #select not None neither other than str
           # filename = directoryTest+"/"+str(row[0])+".txt"
            lrow.append([row[0],row[3],row[4],row[1]])
            
        
    ######################doing the splitting and extraction
    obs ={}
    wait=False
    last =''
    ## preventing any html string to be solved
    lhtml = ['serif', 'font-family'] #string indicator of html fields
    
    res=[] ## list of dictionnary containing all the results

    for obs_id,obs_datetime,patient_id, row in lrow: 
        #dic to store the results for this obs_id
        obs ={} 
    
        ## Work on looking for the POD which is to be an integer in the 10 first characters after the term 'pod'
        if("POD" in row.upper()): 
            i = row.upper().find("POD")
            resPOD = re.search(r'\d+', row[i:i+10])
            if(resPOD != None):
                obs['pod'] = int(resPOD.group()) ## if found add to the result <'pod', X> otherwise don t do nothing
        
        ## Split on all the possible character that can split tokens
        for lineTmp in row.split('\\n'):
            for line in lineTmp.split('\n'):
                for subtTmp in line.split('\t'):
                    for subt in subtTmp.split('\\t'):
                        for subv in subt.split(','):
                           for sub in subv.split(';'):
                             
                             if(not any(word in sub for word in lhtml)): #if there is no sign of being an html line
                                
                                if(wait): #if we are waiting for something then add it to the dictionnary with the correct key
                                    obs[last.strip().lower()] = sub
                                    wait=False #stop waiting
                                    
                                elif(len(sub) > 1 and ':' in sub): # if there is ':' then we re looking for a measerement 
                                    nb = sub.count(':')
                                    start =0
                                    
                                    ## Still to be analysed
                                    if(nb % 2 == 0): #is to be used for the kind of
                                                     # "Assesment : BP : 120"
                                        lword = ['drains', 'assessment', 'objective']
                                       # if(not any(word in sub.lower() for word in lword)):
                                            #print("-----------"+sub)
                                        
                                    ssub = sub.split(':')
                                    last = ssub[0]
                                    if(ssub[1].replace(' ','') == ''):
                                        wait=True
                                    else:
                                        obs[ssub[0].strip().lower()] = ssub[1].strip()
                                elif("POD" in sub.upper() or "POST OP DAY" in sub.upper()):
                                    obs['pod'] = sub # Just in case it remains and we did not found it with the 10 characters rule
    #                            else:
    #                                print(sub)
        if(obs != {}):
            trie = faireTri(obs_id,obs_datetime,patient_id, obs)
            if(trie != None):
                res.append(trie)  
        

    ############# save in a csv file    
    
    listParams = ['obs_id', 'obs_datetime','patient_id', 'pulse', 'bp', 'urine output', 'urine', 'intake', 'temp', 'stoma', 'rt', 'spo2']
    f = open(directoryData+"/extractionText.csv", "w")      
    f.write(';'.join(listParams)+"\n")
    for d in res:
        toPrint=[]
        for p in listParams:
            if(p in d.keys()):
                toPrint.append(str(d[p]))
            else:
                toPrint.append('')
        f.write(';'.join(toPrint)+"\n")
    f.close()    
    print('Complete Extracting the Key Value Pairs')

 


############### function to extract infos
def faireTri(obs_id, obs_datetime,patient_id, obs):
    res = {}
    res['obs_id'] = obs_id
    res['obs_datetime'] = obs_datetime
    res['patient_id'] = patient_id
    key = "pulse"
    if(key in obs.keys() and obs[key] != ''):
        res[key]= int(re.search(r'\d+', obs[key]).group())
           
    key = "bp"
    if(key in obs.keys() and obs[key] != ''):
        if(len(re.findall(r'\d+\/\d+', obs[key]))>0):
            res[key]=re.findall(r'\d+\/\d+', obs[key])[0]
            
    key = "urine output"
    if(key in obs.keys() and obs[key] != ''):
        if(len(re.findall(r'\d+', obs[key]))>0):
            res[key]=re.findall(r'\d+', obs[key])[0]
        elif 'adequate' in obs[key].lower():
             res[key]="adequate"
        elif 'nil' in obs[key].lower() or '-' in obs[key].lower():
             res[key]="nil"  
                
    key = "urine"
    if(key in obs.keys() and obs[key] != ''):
        if(len(re.findall(r'\d+', obs[key]))>0):
            res[key]=re.findall(r'\d+', obs[key])[0]
        elif 'adequate' in obs[key].lower():
             res[key]="adequate"
        elif 'nil' in obs[key].lower() or '-' in obs[key].lower():
             res[key]="nil"     
    
    
    key = "intake"
    if(key in obs.keys() and obs[key] != ''):
        res[key]=re.findall(r'\d+', obs[key])
    
    key = "temp"
    if(key in obs.keys() and obs[key] != ''):
        if(len(re.findall(r'\d+', obs[key]))>0):
            res[key]=int(re.findall(r'\d+', obs[key])[0])
            if(res[key]<50):
                res[key] =int(1.8*res[key]+32)
        
    key = "stoma"
    if(key in obs.keys() and obs[key] != ''):    
        if(len(re.findall(r'\d+', obs[key]))>0):
            res[key]=int(re.findall(r'\d+', obs[key])[0])
            if('litre' in obs[key].lower()):
                res[key] =res[key]*1000
        
    key = "rt"
    if(key in obs.keys() and obs[key] != ''):    
        if(len(re.findall(r'\d+', obs[key]))>0):
            res[key]=int(re.findall(r'\d+', obs[key])[0])
            if('litre' in obs[key].lower()):
                res[key] =res[key]*1000
    
    key = "spo2"
    if(key in obs.keys() and obs[key] != ''):    
        if(len(re.findall(r'\d+', obs[key]))>0):
            res[key]=int(re.findall(r'\d+', obs[key])[0])
    
    if(len(res)>1):
       return res
    else:
        return None
      