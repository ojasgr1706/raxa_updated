
import os
import numpy as np
import datetime

from database import *
from Util import *


def concat_patient_list(sql_query, initial_list):
    res = pd.DataFrame(query(sql_query))
    res.columns = ['patient_id']
    patient_list = res['patient_id'].values.tolist()
    final_list = patient_list + initial_list
    return final_list


def create_todo_df(pathToCCIs):
    sriPatients_df = pd.read_csv('./Anonymized_CCI-CD_without_outliers_ALL.csv')

    # SRI Patient 762 Patients
    sri_patient_list = np.unique(sriPatients_df['patient_id'].values).tolist()

    # Other patient from the Database having Diabetes
    sql_query = 'select distinct person_id from obs where concept_id IN (119481, 175);'
    final_list = concat_patient_list(sql_query, sri_patient_list)

    # print(len(final_list)) # 935

    # Non Diabetic Patients from the Database
    sql_query = 'select distinct person_id from sri_non_diabetic_data'
    final_patientlist = concat_patient_list(sql_query, final_list)

    todo = pd.DataFrame(final_patientlist)
    todo.columns = ['patient_id']
    todo['patient_id'] = todo['patient_id'].astype(int)
    todo = todo.dropna()

    print('Step 1 get All the encounter Id of patients')
    sql_query = ("SELECT patient_id, encounter_id FROM encounter where patient_id in (" + ','.join(
        [str(f) for f in list(todo['patient_id'].values)]) + ')')

    query_result = query(sql_query)
    todo = merge_query_result_df(todo, query_result, ['patient_id', 'encounter_id'], dropna=True)

    print('Step 2 Get all the Records of the patients')
    lres = ['encounter_id', 'concept_id', 'obs_id', 'value_numeric', 'value_text', 'value_coded', 'obs_datetime']
    sql_query = ("SELECT " + ','.join(lres) + " FROM obs where encounter_id in (" + ','.join(
        [str(f) for f in list(todo['encounter_id'].values)]) + ')')
    query_result = query(sql_query)
    todo = merge_query_result_df(todo, query_result, lres, typ=None)  ##### TO REMOVE DROP NA !!!!!!!
    todo = todo[~todo['obs_id'].isnull()]
    todo[['concept_id', 'encounter_id', 'obs_id']] = todo[['concept_id', 'encounter_id', 'obs_id']].astype(int)
    todo = todo.astype(str)
    todo = todo.drop_duplicates(subset=['obs_id'])

    print('Step 3 Excluding some concepts ')
#     Like EncounteHtml and MLEncounterHtml
    todo = todo[(todo['concept_id'] != '33334452') &
                (todo['concept_id'] != '33334004') &
                (todo['concept_id'] != '33334003') &
                (todo['concept_id'] != '170250004') &
                (todo['concept_id'] != '33334002')
                ]
    todo = todo[
        ((todo['concept_id'] == '160632') & (todo['value_text'] != 'None')) | ((todo['concept_id'] != '160632'))]

    print('Step 4 Running the Key-Value Extractor')
    cnx = mysql.connector.connect(user=userdb, password=pwddb, host=hostdb, database=databasedb)
    cursor = cnx.cursor()
    runKVExtractor('./resultDir', cursor, todo)
    tmp = pd.read_csv('./resultDir/extractionText.csv', sep=';')
    del tmp['obs_id']  # TODO clean
    todo['i'] = 0
    tmp['i'] = 1

    tmp['patient_id'] = tmp['patient_id'].astype(int)
    todo['patient_id'] = todo['patient_id'].astype(int);

    todo = pd.merge(todo, tmp, on=['i', 'patient_id', 'obs_datetime'], how='outer')
    del todo['i']
    todo['patient_id'] = todo['patient_id'].astype(str)
    todo['value_text'] = todo['value_text'].astype(str)
    todo['obs_datetime'] = todo['obs_datetime'].apply(
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").date())
    return todo