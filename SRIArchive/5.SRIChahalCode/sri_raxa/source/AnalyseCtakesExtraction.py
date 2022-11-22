import config as config
import pandas as pd
import mysql.connector
import os
import requests
from flask import request, json, jsonify

currentDirectoryPath = os.path.dirname(os.path.abspath(__file__))
resultsDirectory = currentDirectoryPath + '/resultDir/'
userdb = config.userdb
pwddb = config.pwddb
hostdb = config.hostdb
databasedb = config.databasedb


class CtakesData():

    def __init__(self, name, start_index, end_index, value=[]):
        self.name = name
        self.value = value
        self.start_index = start_index
        self.end_index = end_index

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, CtakesData):
            return self.start_index == other.start_index and self.end_index == other.end_index and self.name == other.name
        return False


# Ctakes post Call local
def ctakes_post_call(data):
    headers = {
        'Accept': "application/json;charset=UTF-8",
        'Content-Type': "application/json",
        'Authorization': 'Basic YWo6cXdhcw=='
    }

    json_dic = {'text': data}
    payload = json.dumps(json_dic)

    print(payload)
    print()
    print()

    url = config.raxa_ctakes_url
    ctakes_result = requests.post(url, data=payload, headers=headers)
    ctakes_data = ctakes_result.json()
    return ctakes_data


def addDataToList(name, ctakes_dic, ctakes_data_obj_list):
    if name in ctakes_dic and 'startIndex' in ctakes_dic and 'endIndex' in ctakes_dic:
        data = ctakes_dic[name]
        start_index = ctakes_dic['startIndex']
        end_index = ctakes_dic['endIndex']

        if 'value' in ctakes_dic:
            value = ctakes_dic['value']
            c1 = CtakesData(data, start_index, end_index, value)
        else:
            c1 = CtakesData(data, start_index, end_index)

        if c1 not in ctakes_data_obj_list:
            ctakes_data_obj_list.append(c1)

    return ctakes_data_obj_list


# Parse the Ctakes Data and return the Res_list
def parse_res_to_list(ctakes_data):
    ctakes_data_obj_list = []

    for key, value in ctakes_data.items():
        ctakes_value = value

        if len(ctakes_value) > 0:
            for i in ctakes_value:
                print(i, type(i))
                try:
                    ctakes_dic = json.loads(i)
                    if isinstance(ctakes_dic, dict):
                        if 'name' in ctakes_dic:
                            ctakes_data_obj_list = addDataToList('name', ctakes_dic, ctakes_data_obj_list)

                        # if 'arg1' in ctakes_dic:
                        #     ctakes_data_obj_list = addDataToList('arg1', ctakes_dic, ctakes_data_obj_list)
                        #
                        # if 'arg2' in ctakes_dic:
                        #     ctakes_data_obj_list = addDataToList('arg1', ctakes_dic, ctakes_data_obj_list)

                except ValueError:
                    print(i)

    return ctakes_data_obj_list


def get_ctakes_count(ctakes_res_list):
    ctakes_detection_count = 0

    def get_value_len(value_list):
        count = 0
        for i in value_list:
            count += len(i)

        return count

    for obj in ctakes_res_list:
        obj_len = len(obj.name) + get_value_len(obj.value)
        ctakes_detection_count = ctakes_detection_count + obj_len
    return ctakes_detection_count


# Return Structure Text count and Ctakes Json
def get_count_structure_Text(row):
    ctakes_text = row.replace("\n", ",")

    # Get the Ctakes Json for This Observation Text
    ctakes_json_data = ctakes_post_call(ctakes_text)

    # Get the ctakes resList of Containing obj of Class CtakesData
    ctakes_res_list = parse_res_to_list(ctakes_json_data)

    # Get ctakes detection count for the Given list
    ctakes_detection_count = get_ctakes_count(ctakes_res_list)

    return (ctakes_detection_count, ctakes_json_data)


def assignDiabetesLabel(todo):
    final = pd.DataFrame()
    final['patient_id'] = todo['patient_id'].drop_duplicates()
    final = final.reset_index(drop=True)
    final['hasDiabetesMellitus'] = 0

    temp = list(todo['patient_id'][(todo['concept_id'] == '119481')])

    diabetes_patient_list = []

    for i in temp:
        if i not in diabetes_patient_list:
            diabetes_patient_list.append(i)

    def assgin_new_concept(x):
        # print(x[0], x[1])
        if x[0] in diabetes_patient_list:
            return 1
        else:
            return 0

    # Setting hasDiabetesMellitus 1 for those columns having concept_id 119481
    final['hasDiabetesMellitus'] = final[['patient_id', 'hasDiabetesMellitus']].apply(assgin_new_concept, axis=1)

    return final


def getDataFromPatientId(isAnalyseCtakes):
    todo = pd.read_pickle(resultsDirectory + 'tododiabetes.pckl')


    todo = assignDiabetesLabel(todo)


    todo = todo[todo['hasDiabetesMellitus'] == 1]
    print(todo.shape)

    if isAnalyseCtakes:
        res = getPatientDataFromMysql(todo)
        res['total_character'] = res['value_text'].apply(lambda x: len(x))

        # ctakes_post_call(res['value_text'][0:1].values)
        res['detected_character_count'], res['ctakes_text'] = zip(
            *res['value_text'].apply(lambda x: get_count_structure_Text(x)))

        res.to_pickle('ctakes_extraction.pckl')
        print('Success')
    else:
        res = pd.read_pickle('ctakes_extraction.pckl')

    return res


def getPatientDataFromMysql(todo):
    final = pd.DataFrame()
    final['patient_id'] = todo['patient_id'].drop_duplicates()
    final = final.reset_index(drop=True)

    cnx = mysql.connector.connect(user=userdb, password=pwddb, host=hostdb, database=databasedb)
    cursor = cnx.cursor()

    query = (
                'SELECT obs_id, person_id, value_text from obs where concept_id =160632 and value_text is not null and value_text '
                '!="" and '
                'voided = '
                '0 '
                'and person_id '
                'in (' + ','.join([str(f) for f in list(final['patient_id'].values)]) + ')' + ' order by person_id')
    cursor.execute(query)

    res = pd.DataFrame(cursor.fetchall())
    res.columns = ['obs_id', 'patient_id', 'value_text']
    return res


if __name__ == '__main__':
    # get_count_structure_Text('Fever Fever Fever for 3 days')
    res = getDataFromPatientId(False)

    result = sum(res['detected_character_count'].values) / sum(res['total_character'].values)

    print('Coverage of data by ctakes: ', result * 100, '%');


