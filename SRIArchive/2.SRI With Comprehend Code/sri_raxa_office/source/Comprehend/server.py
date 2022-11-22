import config as config
from flask import Flask, request, json, jsonify
from flask_cors import CORS

import boto3
from pprint import pprint
from database import query
import json
import os
currentDirectoryPath = os.path.dirname(os.path.abspath(__file__))
import os


Path = os.path.dirname(os.path.abspath(__file__))
comprehend_jsons_dir = Path + '/comprehendResponse/'

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

# Index


def comprehend_call_save_response(obs_id, value_text):
    response = comprehend_medical_call(value_text)
    if response is not None:
        save_comprehend_response(obs_id, response)
    return response


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
    with open(comprehend_jsons_dir + file_name, 'w') as outfile:
        json.dump(data, outfile)


# Index
@app.route('/', methods=['GET', 'POST'])
def index():
    return "Flask is running fast"


@app.route('/comprehend', methods=['POST'])
def get_comprehend_json():
    data = request.data
    body_dic = json.loads(data)

    obs_id = body_dic['encounter_id']
    value_text = body_dic['value_text']

    print(obs_id, value_text)

    file_name = str(obs_id) + ".json"

    comprehend_jsons_list = [f for f in os.listdir(comprehend_jsons_dir)]
    comprehend_result = {}

    if file_name in comprehend_jsons_list:
        file = open(comprehend_jsons_dir + file_name)
        file_output = file.read()
        comprehend_result = json.loads(file_output)
        print(comprehend_result)
    else:
        print('Comprehend rest call and saving Response')
        comprehend_result = comprehend_call_save_response(obs_id, value_text)

    return jsonify(comprehend_result)


app.debug = True
if __name__ == '__main__':
    app.run(host=config.myIp, port=5095)
    # get_comprehend_json()
