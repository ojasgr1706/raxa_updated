import pandas as pd
import numpy as np
from Util import unique, extract_value_normalize_date
from variables import list_to_avoid
from database import query
import json


def create_dataframe_from_dic(dic, patient_id, obs_id):
    values = [v for key, v in dic.items()]
    values.insert(0, patient_id)
    values.insert(1, obs_id)
    dataFrame_columns = [key for key, v in dic.items()]
    dataFrame_columns.insert(0, 'patient_id')
    dataFrame_columns.insert(1, 'obs_id')
    todo = pd.DataFrame(values).transpose()
    todo.columns = dataFrame_columns
    return todo


def create_list_from_ctakes_response(obs_id, res):
    if not obs_id:
        return []
    else:
        ctakes_key_value_list = []

        if len(res) <= 0:
            pass
        else:
            res_obj = json.loads(res)

            parseMentionList = ['LabValueMentionList', 'ProcedureMentionList', 'SignSymptomMentionList' , 'DiseaseDisorderMentionList' , 'DrugNerMentionList']

            # pprint(res_obj)

            for key in res_obj.keys():
                mention_list = res_obj[key]
                if key in parseMentionList:
                    for i in mention_list:
                        try:
                            mention_obj = json.loads(i)
                            # print(mention_obj['name'], end=" ")
                            ctakes_word = mention_obj['name']
                            ctakes_value = None

                            if 'value' in mention_obj and len(mention_obj['value']) > 0:
                                # print(mention_obj['value'])
                                ctakes_value = mention_obj['value']

                            ctakes_key_value_list.append((ctakes_word, ctakes_value))
                        except ValueError:
                            print('Parse Error while parsing ', i)

        return ctakes_key_value_list


def extact_value_concepts_from_freetext(obs_id, res):
    if not obs_id:
        return None
    else:
        # Get the unique obs_ids
        res = create_list_from_ctakes_response(obs_id, res)
        res = unique(res)

        # print(res)

        df = pd.DataFrame(res)

        res_ctakes_list = []
        key_value_dic = {}

        if not df.empty:
            df.columns = ['value_text', 'concept_value']

            def extract_dictionary(row, res_ctakes_list_arg, key_value_dic_arg):
                value = list(row)
                key = value[0].lower()

                if key not in list_to_avoid:
                    # Can return None values
                    if key.lower() == 'bp' and value[1] is not None and len(value[1]) > 1:
                        sbp_key = 'Systolic blood pressure'
                        dbp_key = 'Diastolic blood pressure'
                        sbp_value = float(value[1][0])
                        dbp_value = float(value[1][1])

                        value_array = []
                        if sbp_key in key_value_dic_arg:
                            value_array = key_value_dic_arg[sbp_key]
                            value_array.append(sbp_value)
                            key_value_dic_arg[sbp_key] = value_array
                        else:
                            value_array.append(sbp_value)
                            key_value_dic_arg[sbp_key] = value_array

                        value_array = []
                        if dbp_key in key_value_dic_arg:
                            value_array = key_value_dic_arg[sbp_key]
                            value_array.append(dbp_value)
                            key_value_dic_arg[dbp_key] = value_array
                        else:
                            value_array.append(dbp_value)
                            key_value_dic_arg[dbp_key] = value_array
                    else:
                        key_value = extract_value_normalize_date(value[1])

                        # if not values exist then put it in the ctakes List
                        if key_value is None or str(key_value) is 'nan':
                            res_ctakes_list_arg.append(key)
                        else:
                            # if values exists put it in the dictionary
                            value_array = []
                            if key in key_value_dic_arg:
                                value_array = key_value_dic_arg[key]
                                value_array.append(key_value)
                                key_value_dic_arg[key] = value_array
                            else:
                                value_array.append(key_value)
                                key_value_dic_arg[key] = value_array
                else:
                    pass

            df[['value_text', 'concept_value']].apply(lambda x: extract_dictionary(x, res_ctakes_list, key_value_dic),axis=1)

            # Finding the mean of values in Dictionaries
            for k, v in key_value_dic.items():
                value_list = key_value_dic[k]
                print(k, value_list)

                mean_value = np.mean(value_list)
                key_value_dic[k] = mean_value

            # Finding Unique keywords in Ctakes List
            res_ctakes_list = unique(res_ctakes_list)

            # print(res_ctakes_list)
            # print(key_value_dic)
        else:
          pass

    return key_value_dic, res_ctakes_list
