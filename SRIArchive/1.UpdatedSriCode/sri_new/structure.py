from Util import bind_list, get_value_from_num, get_value_from_bool, extract_key_value, get_plt
from assignCD import assing_maxcd_number
from database import query
import pandas as pd
from Util import unique, extract_value_normalize_date
import re


# For the structure Concepts from the database
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


# Add the structure concepts from db
def add_structure_concepts_from_db(todo, last, funToMerge =lambda x :x):
    res = []
    col = []
    enc = 0  # to run through the list of last

    # To correct the location of numerical values
    todo['value_numeric'][todo['concept_id' ]=='33334012'] = todo['value_text'][todo['concept_id' ]=='33334012']
    for k, v in numDic.items():
        col.append(v)
        res.append(funToMerge(bind_list(last[enc] ,get_value_from_num(todo, k, list))))
        enc = enc + 1

    # Boolean
    for k, v in boolDic.items():
        col.append(v)
        res.append(funToMerge(bind_list(last[enc], get_value_from_bool(todo, k, list))))
        enc = enc + 1

    # KVExtracted
    for l in kvList:
        col.append(l)
        res.append(funToMerge(bind_list(last[enc], extract_key_value(todo, l, list))))
        enc = enc + 1

    # PLT (Platelets), concept_id=1026
    res.append(funToMerge(bind_list(last[enc], get_plt(todo))))
    enc = enc + 1

    # newNLP: 1: 'words', 2: 'maxCD', 3: 'maxCDN'
    new_nlp = assing_maxcd_number(todo)
    tmp_res = [bind_list(last[enc], new_nlp[0])]

    enc = enc + 1
    # tmpRes.append(myMax(last[enc], newNLP[1]))
    # enc=enc+1
    # tmpRes.append(myMax(last[enc], newNLP[2]))
    # enc=enc+1
    # tmpRes.append(newNLP[1])
    # tmpRes.append(newNLP[2])
    res = res + tmp_res
    return res


# Get the non- value Ctakes Keyowords
def get_non_value_concept(res):
    if not res:
        return []
    else:
        # sql_query = ('SELECT value_text from mlob where value_text is not null and value_text != "" and obs_id = ' + str(obsId))
        # res = query(sql_query)

        final_res = create_list_from_result(res)
        return final_res


def create_list_from_result(res):
    final_result_set = set()
    for i in res:
        list_i = list(i)
        for j in list_i:
            final_result_set.add(j)
    result_list = list(final_result_set)
    result_list = remove_value_keywords(result_list)
    return result_list


def remove_value_keywords(ctakes_list):
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


# For Update now list from mlob

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


def get_data_for_obs_ids(obs_ids):
    if obs_ids:
        sql_query = (
            "SELECT m.obs_id, value_text, concept_value, m.concept_id, c.datatype_id from mlob as m inner join "
            "concept as c on m.concept_id = c.concept_id  where obs_id in ({0})".format(
                ','.join([str(f) for f in obs_ids])))

        res = query(sql_query)
        return res
    else:
        return None


# def update_num_list(single_todo_df, old_now, row_list):
#     if row_list:
#         # sql_query = ("SELECT value_text, concept_value, m.concept_id, c.datatype_id from mlob as m inner join concept "
#         #              "as c on m.concept_id = c.concept_id "
#         #              + " where obs_id = " + str(obs_id))
#
#         old_now = update_concepts_from_mlob(row_list, old_now, single_todo_df)
#         return old_now
#     else:
#         return old_now


# Update Concepts from mlob
def update_concepts_from_mlob(result_list, old_now, single_todo_df):
    patient_id = list(single_todo_df['patient_id'][0:1])[0]

    result_list = unique(result_list)
    lres = []

    for result in result_list:
        concept_name = result[1]
        freetext_concept_value = result[2]
        datatype_id = result[4]

        if concept_name:
            key = concept_name.lower()
            if freetext_concept_value:
                if key in listNumDic:
                    column_index = int(listNumDic[key])

                    # Value not present at the Index then try to find from the free_text
                    if old_now[column_index] is None:
                        extracted_concept_value = extract_value_normalize_date(freetext_concept_value)

                        # if Concept DataType is N/A then skip them
                        if datatype_id == 4:
                            # print('Data type Id 4: for ', concept_name)
                            pass
                        else:
                            if key.lower() == 'rt':
                                pass
                            elif key.lower() == 'temp' and extracted_concept_value < 40.0:
                                # Change in Temperature in Fahrenheit
                                print("Temp in Celcius with value: " + str(extracted_concept_value) + " for patientId " + str(patient_id)
                                      + " at index: " + str(column_index))
                                extracted_concept_value = (extracted_concept_value * 1.8) + 32
                                old_now[column_index] = wrap_datetime(extracted_concept_value, single_todo_df)
                            else:
                                old_now[column_index] = wrap_datetime(extracted_concept_value, single_todo_df)
                    else:
                        pass
            else:
                if concept_name not in lres:
                    lres.append(concept_name)

    lres = remove_value_keywords(lres)

    return old_now, lres


def wrap_datetime(concept_value, single_todo_df):
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


def combined_ctakesResponse_for_obs_ids(obs_ids):
    if obs_ids:
        sql_query = ("SELECT DISTINCT ctakes_response, obs_id FROM mlob WHERE ctakes_response is not null "
                     "AND ctakes_response !='' AND "
                     "concept_id NOT IN ('33334452', '33334004', '33334003', '170250004', '170250005') " 
                     "AND obs_id IN ({0})".format(','.join([str(f) for f in obs_ids])))
        res = query(sql_query)
        return res
    else:
        return None
