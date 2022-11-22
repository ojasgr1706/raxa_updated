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
import config as config
import mysql.connector
import requests
import re
from toolsTodoSummedUp import *
from KVExtractor import *

######################################### Database Credentials
userdb = config.userdb
pwddb = config.pwddb
hostdb = config.hostdb
databasedb = config.databasedb

#########################################

listNumDic = {'weight': '0', 'height': '1', 'serum_sodium': '2',
              'serum_potassium': '3',
              'serum_creatinine': '4',
              'serum_calcium': '5',
              'hemoglobin': '6',
              'phosphorous test': '7',
              'wbc': '8',
              'urea': '9',
              'serum_albumin': '10',
              'direct_bilirubin': '11',
              'sgpt': '12',
              'bilirubin': '13',
              'sgot': '14',
              'total_protein': '15',
              'alkaline_phosphatase': '16',
              'serum_ggt': '17',
              'prothrombin': '18',
              'pulse': '25',
              'bp': '26',
              'urine output': '27',
              'urine': '28',
              'intake': '29',
              'temp': '30',
              'stoma': '31',
              'rt': '32',
              'spo2': '33'
              }


def add_concept_form_db(pathToCCIs):
    listPatient = pd.read_csv(pathToCCIs, sep=',')
    cnx = mysql.connector.connect(user=userdb, password=pwddb, host=hostdb, database=databasedb)
    cursor = cnx.cursor()

    # check if the 'patient_id' is in the columns of the csv sheet and ifnot it will build it from the databse

    if ('patient_id' not in listPatient.columns):
        print('using patient id from the database...')
        svg = listPatient[['Reg no.', 'CD']]
        todo = listPatient[['Reg no.']]
        query = ("SELECT value, person_id FROM person_attribute where person_attribute_type_id=110")
        cursor.execute(query)
        todo = mergeNtype(todo, cursor, ['Reg no.', 'patient_id'], dropna=True)
    else:
        print('using patient id from the spreadsheet...')
        todo = listPatient[['Reg no.', 'patient_id']].dropna().astype(int)
    print('step 1')

    # gets all the encounter_id
    query = ("SELECT patient_id, encounter_id FROM encounter where patient_id in (" + ','.join(
        [str(f) for f in list(todo['patient_id'].values)]) + ')')
    cursor.execute(query)
    todo = mergeNtype(todo, cursor, ['patient_id', 'encounter_id'], dropna=True)  ##### TO REMOVE DROP NA !!!!!!!
    print('step 2')

    # get all the records for our patients only
    lres = ['encounter_id', 'concept_id', 'obs_id', 'value_numeric', 'value_text', 'value_coded', 'obs_datetime']
    query = ("SELECT " + ','.join(lres) + " FROM obs where encounter_id in (" + ','.join(
        [str(f) for f in list(todo['encounter_id'].values)]) + ')')
    cursor.execute(query)
    todo = mergeNtype(todo, cursor, lres, typ=None)  ##### TO REMOVE DROP NA !!!!!!!
    todo = todo[~todo['obs_id'].isnull()]
    todo[['concept_id', 'encounter_id', 'obs_id']] = todo[['concept_id', 'encounter_id', 'obs_id']].astype(int)
    todo = todo.astype(str)
    todo = todo.drop_duplicates(subset=['obs_id'])
    print('step 3')

    # exclude some  concept_id : html ones
    todo = todo[(todo['concept_id'] != '33334452') &
                (todo['concept_id'] != '33334004') &
                (todo['concept_id'] != '33334003') &
                (todo['concept_id'] != '170250004')
                ]
    todo = todo[
        ((todo['concept_id'] == '160632') & (todo['value_text'] != 'None')) | ((todo['concept_id'] != '160632'))]

    print('step 4')
    # run the KV extractor to build directly the key,value columns
    runKVExtractor('./resultDir', cursor, todo)
    tmp = pd.read_csv('./resultDir/extractionText.csv', sep=';')
    del tmp['obs_id']  # TODO clean
    todo['i'] = 0
    tmp['i'] = 1
    todo = pd.merge(todo, tmp, on=['i', 'patient_id', 'obs_datetime'], how='outer')
    del todo['i']
    todo['patient_id'] = todo['patient_id'].astype(str)
    todo['value_text'] = todo['value_text'].astype(str)
    todo['obs_datetime'] = todo['obs_datetime'].apply(
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").date())
    return todo


def remove_key_words(ctakes_list):
    kvList = ['pulse', 'bp', 'urine output', 'urine', 'intake', 'temp', 'stoma', 'rt', 'spo2',
              'pod', 'weight', 'height', 'serum_sodium', 'serum_potassium', 'serum_creatinine', 'serum_calcium',
              'hemoglobin', 'phosphorous_test', 'wbc', 'urea', 'serum_albumin', 'direct_bilirubin', 'sgpt', 'bilirubin',
              'sgot',
              'total_protein', 'alkaline_phosphatase', 'serum_ggt', 'prothrombin', 'phosphorous test']
    res_list = []
    for keyword in ctakes_list:
        if keyword.lower() not in kvList:
            res_list.append(keyword)
    return res_list


def create_list_from_result(res):
    final_result_set = set()
    for i in res:
        list_i = list(i)
        for j in list_i:
            final_result_set.add(j)

    result_list = list(final_result_set)
    result_list = remove_key_words(result_list)
    print(result_list)
    return result_list


def add_concepts_from_ctakes(single_todo_df, old_now, obs_ids):
    if obs_ids:
        cnx = mysql.connector.connect(user=userdb, password=pwddb, host=hostdb, database=databasedb)
        cursor = cnx.cursor()
        sql_query = ("SELECT value_text, concept_value, concept_id from mlob where obs_id in (" + ','.join(
            [str(f) for f in obs_ids]) + ')')
        cursor.execute(sql_query)
        res = cursor.fetchall()
        old_now = check_concepts_from_mlob(res, old_now, single_todo_df)
        return old_now
    else:
        return old_now


def generate_value(concept_value, single_todo_df):
    single_todo_df['ctakes_value'] = concept_value
    tmp = single_todo_df[['obs_datetime', 'ctakes_value']][single_todo_df['concept_id'] == '160632']

    if not tmp.empty:
        tmp['tot'] = tmp.apply(lambda x: (x[0], x[1]), axis=1)
        res = []
        res.append(tmp['tot'].tolist()[0])
        del tmp['tot']
        del single_todo_df['ctakes_value']
        return res
    else:
        return None


def check_concepts_from_mlob(res, old_now, single_todo_df):
    for i in res:
        result_list = list(i)
        value_text = result_list[0]
        concept_value = None

        # If concept Value present
        if result_list[1]:
            concept_value = extract_value(result_list[1])
            concept_id = result_list[2]

            key = value_text.lower()

            if concept_value is not None:
                # Check if given concept occur in dictionary
                if key in listNumDic:
                    column_index = int(listNumDic[key])
                    print('Key: ' + key + 'Index ' + str(column_index) + 'Value: ' + str(concept_value))
                    # Check if at that position old_now is None
                    if old_now[column_index] is None:
                        old_now[column_index] = generate_value(concept_value, single_todo_df)


    return old_now


# def extract_value(string):
#     new_str = string.replace("[", "").replace("]", "").replace('"', '')
#     new_str = new_str.split(",")
#     try:
#         concept_value = float(new_str[0])
#     except ValueError as ex:
#         print(ex)
#         concept_value = None
#
#     return concept_value

def extract_value(string):
    numlist = re.findall('\d+', string)
    concept_value = None
    if numlist and len(numlist) > 0:
        try:
            concept_value = float(numlist[0])
        except ValueError as ex:
            print(ex)
    return concept_value

def fetch_mlob_for_obs_ids(obsIds):
    print(obsIds)
    if not obsIds:
        return []
    else:
        cnx = mysql.connector.connect(user=userdb, password=pwddb, host=hostdb, database=databasedb)
        cursor = cnx.cursor()
        sql_query = ("SELECT value_text from mlob where obs_id in (" + ','.join([str(f) for f in obsIds]) + ')')
        # print(sql_query)
        cursor.execute(sql_query)
        res = cursor.fetchall()
        final_res = create_list_from_result(res)
        return final_res


def ctakes_post_call(text):
    headers = {'Accept': 'application/json;charset=UTF-8',
               'Content-Type': 'application/json',
               'Authorization': 'Basic YWo6cXdhcw=='
               }
    # splitted_words_list = text.splitlines()
    # single_line_text = ",".join(splitted_words_list)
    # print(single_line_text)
    payload = '{ "text": "' + text + '"}'
    url = config.raxa_ctakes_url
    try:
        ctakes_result = requests.post(url, data=payload, headers=headers)
    except requests.exceptions.RequestException as e:
        print(e)
    ctakes_result_json = ctakes_result.json()
    # print(ctakes_result_json)
    return ctakes_result_json


def makeTodoRunServer(listPat):
    cnx = mysql.connector.connect(user=userdb, password=pwddb, host=hostdb, database=databasedb)
    cursor = cnx.cursor()

    # check if the 'patient_id' is in the columns of the csv sheet and ifnot it will build it from the databse
    todo = pd.DataFrame({'patient_id': listPat})

    print('Getting patient_id and encounter from Database')
    # gets all the encounter_id
    query = ("SELECT patient_id, encounter_id FROM encounter where patient_id in (" + ','.join(
        [str(f) for f in list(todo['patient_id'].values)]) + ')')
    cursor.execute(query)
    todo = pd.DataFrame(cursor.fetchall())
    todo.columns = ['patient_id', 'encounter_id']

    # get all the records for our patients only
    lres = ['encounter_id', 'concept_id', 'obs_id', 'value_numeric', 'value_text', 'value_coded', 'obs_datetime']
    query = ("SELECT " + ','.join(lres) + " FROM obs where encounter_id in (" + ','.join(
        [str(f) for f in list(todo['encounter_id'].values)]) + ')')
    cursor.execute(query)
    todo = mergeNtype(todo, cursor, lres, typ=None)  ##### TO REMOVE DROP NA !!!!!!!
    todo = todo[~todo['obs_id'].isnull()]
    todo[['concept_id', 'encounter_id', 'obs_id']] = todo[['concept_id', 'encounter_id', 'obs_id']].astype(int)
    todo = todo.astype(str)
    todo = todo.drop_duplicates(subset=['obs_id'])

    print('Excluding MLEncounterHTML and other concepts')
    # exclude some  concept_id : html ones
    todo = todo[(todo['concept_id'] != '33334452') &
                (todo['concept_id'] != '33334004') &
                (todo['concept_id'] != '33334003') &
                (todo['concept_id'] != '170250004')]

    todo = todo[
        ((todo['concept_id'] == '160632') & (todo['value_text'] != 'None')) | ((todo['concept_id'] != '160632'))]

    print('Running KV Extractor for getting Key Value Pairs')
    # run the KV extractor to build directly the key,value columns save it in a File and Read it to make todo DataFrame
    runKVExtractor('./resultDir', cursor, todo)
    tmp = pd.read_csv('./resultDir/extractionText.csv', sep=';')
    del tmp['obs_id']  # TODO clean
    todo['i'] = 0
    tmp['i'] = 1
    todo = pd.merge(todo, tmp, on=['i', 'patient_id', 'obs_datetime'], how='outer')
    del todo['i']
    todo['patient_id'] = todo['patient_id'].astype(str)
    todo['value_text'] = todo['value_text'].astype(str)
    todo['obs_datetime'] = todo['obs_datetime'].apply(
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").date())
    return todo
