import boto3
from pprint import pprint
from database import query
import json
import os
currentDirectoryPath = os.path.dirname(os.path.abspath(__file__))



def comprehend_call_save_response(obs_id, value_text):
    response = comprehend_medical_call(value_text)
    if response is not None:
        save_comprehend_response(obs_id, response)


def comprehend_medical_call(value_text):
    if value_text is not None and len(value_text) > 10:
        client = boto3.client(service_name='comprehendmedical', region_name='us-east-1')
        result = client.detect_entities(Text=str(value_text))
        entities = result['Entities'];
        for entity in entities:
            pprint(entity)

        return entities
    else:
        return None


def save_comprehend_response(obs_id, data):
    file_name = str(obs_id) + '.json'
    print('Saving file: ' + file_name)
    with open(file_name, 'w') as outfile:
        json.dump(data, outfile)



# def get_value_text_for_analysis():
#     sql_query = "Select obs_id, value_text from obs where person_id " \
#                 "IN (select distinct person_id from sri_non_diabetic_data) " \
#                 "and concept_id = 160632 and value_text is not null " \
#                 "and CHAR_LENGTH(value_text) > 20 limit 10"
#
#     res = query(sql_query)
#     text_list = []
#
#     for row in res:
#         print(row[1])
#         text_list.append(row[1])
#
#
# get_value_text_for_analysis()
