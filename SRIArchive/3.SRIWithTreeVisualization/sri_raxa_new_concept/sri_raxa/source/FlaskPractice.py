import config as config
from flask import Flask, request, json, jsonify
import requests
import mysql.connector
import os
import pandas as pd
from makeTodo import fetch_mlob_for_obs_ids

raxa_ctakes_url = "http://localhost:8080/openmrs/"
currentDirectoryPath = os.path.dirname(os.path.abspath(__file__))
pathTocTakes = currentDirectoryPath + '/tmp/'
userdb = config.userdb
pwddb = config.pwddb
hostdb = config.hostdb
databasedb = config.databasedb
resultsDirectory = currentDirectoryPath + '/resultDir/'
app = Flask(__name__)


# Index


# Index
@app.route('/', methods=['GET', 'POST'])
def index():
  return "Flask is running fast"


def most_used_concept():
  cnx = mysql.connector.connect(user=userdb, password=pwddb, host=hostdb, database=databasedb)
  cursor = cnx.cursor()
  sql_query = (
    "select count(concept_id) as ConceptCount, concept_id from obs where concept_id not in(160632, 170250004) group by concept_id having count(concept_id) order by ConceptCount desc")
  cursor.execute(sql_query)
  res = cursor.fetchall()
  conceptCountList = []
  concept_ids = []
  max_count = 0
  for i in res:
    list_i = list(i)
    conceptCountList.append(list_i)
    max_count = max(max_count, list_i[0])
    concept_ids.append(list_i[1])

  sql_query2 = ("SELECT name, concept_id from concept_name where concept_id in (" + ','.join(
    [str(f) for f in concept_ids]) + ')')

  cursor.execute(sql_query2)
  conceptNameList = cursor.fetchall()

  for j in conceptNameList:
    list2_i = list(j)
    for index in range(0, len(conceptCountList)):
      if conceptCountList[index][1] == list2_i[1]:
        conceptCountList[index].append(list2_i[0])

  for concept_name in conceptCountList:
    print(concept_name)

  df = pd.DataFrame(conceptCountList)

  df.to_csv('common_concept.csv');

  return conceptCountList


@app.route("/patientRestCall", methods=['GET'])
def patientListRestCall():
    todo = pd.read_pickle(resultsDirectory + 'tododiabetes.pckl')
    final = pd.DataFrame()
    final['patient_id'] = todo['patient_id'].drop_duplicates()
    final = final.reset_index(drop=True)

    patient_list = final['patient_id'].tolist()



    cnx = mysql.connector.connect(user=userdb, password=pwddb, host=hostdb, database=databasedb)
    cursor = cnx.cursor()

    query = (
            'SELECT obs_id from obs where concept_id =160632 and value_text is not null and value_text !="" and '
            'voided = '
            '0 '
            'and person_id '
            'in (' + ','.join([str(f) for f in list(final['patient_id'].values)]) + ')'+ ' order by obs_id')
    cursor.execute(query)

    res = pd.DataFrame(cursor.fetchall())
    res.columns = ['obs_id']

    print('Length of  ObsIds ', res.shape[0])

    # start = int(request.args.get('start'))
    # end   = int(request.args.get('end'))
    # print(start)
    # print(end)
    res = res[15001:143174]
    obs_ids = res['obs_id'].tolist()

    # print(obs_ids)

    return jsonify({"obsIds": obs_ids})

def getTemp():
  def findmaxCDN(res):
    maxCDN = 0
    for i in res['PODArray']:
      if i['maxCDN'] > maxCDN:
        maxCDN = i['maxCDN']
    return maxCDN

  def getPatientCDN(patient_id, port):
    url = "http://192.168.0.103:" + port + "/prediction"
    querystring = {"patientId": patient_id}
    payload = ""
    headers = {
      'Content-Type': "application/json",
      'Accept': "application/json;charset=UTF-8",
      'cache-control': "no-cache"
    }
    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    json_data = json.loads(str(response.text))
    return response

  patient_id = "8595"
  result1 = getPatientCDN(patient_id, "5045")
  result2 = getPatientCDN(patient_id, "5055")


@app.route('/temp', methods=['GET'])
def tempReadFile():
  # res = most_used_concept()
  getTemp()
  return "Success reading file"


@app.route('/freetext', methods=['POST'])
def multiline_to_singleLine():
  file = open(pathTocTakes + "/avant" + "/" + "8073,0", "r")
  fileOutput = file.read()
  str = fileOutput
  lines = str.splitlines()
  ctakes_text = ','.join(lines)
  print(ctakes_text)
  print(fileOutput)
  return "Success"


def ctakes_post_call():
  headers = {
    'Accept': "application/json;charset=UTF-8",
    'Content-Type': "application/json"
  }

  if request.headers['Authorization']:
    headers['Authorization'] = request.headers['Authorization']
  else:
    headers['Authorization'] = 'Basic YWo6cXdhcw=='

  data = request.data
  dataDict = json.loads(data)
  text = dataDict['text']

  payload = '{ "text": "' + text + '"}'
  url = raxa_ctakes_url + "ws/rest/v1/raxacore/cds/freetext"
  ctakes_result = requests.post(url, data=payload, headers=headers)
  ctakes_data = ctakes_result.json()
  ctakes_data = parse_ctakes_result(ctakes_data)
  print(ctakes_data)
  return jsonify(ctakes_data)


def parse_ctakes_result(ctakes_data):
  res = []
  mentionAnnotationType = ["DrugChangeStatusAnnotation", "LabValueMentionList",
                           "SignSymptomMention", "DrugNerMentionList", "ProcedureMention"]

  resWithoutDuplicates = []
  for annotation in ctakes_data:
    if annotation in mentionAnnotationType:
      for value in ctakes_data[annotation]:
        print(value)
        try:
          preferredText = json.loads(value)
          print(preferredText['labName'])
          res.append(preferredText['labName'])
        except Exception as e:
          print(e)

  for tempRes in res:
    if tempRes not in resWithoutDuplicates:
      resWithoutDuplicates.append(tempRes)
  return resWithoutDuplicates


app.debug = True
if __name__ == '__main__':
  app.run(host=config.myIp, port=5075)
  most_used_concept()
