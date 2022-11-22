import config as config
from os import listdir
from flask import Flask, request, jsonify
from prediction_splitted import *
from sklearn.preprocessing import Imputer
from splittingPOD import *

# Global Variables
currentDirectoryPath = os.path.dirname(os.path.abspath(__file__))
resultsDirectory = currentDirectoryPath + '/resultDir/'
currentDirectoryPath = os.path.dirname(os.path.abspath(__file__))
pathToPOD = currentDirectoryPath + '/resultDir/'  # Contain the result of splittingPOD.py
version = '13-new-ctakes_new_Pat'
trainingResult = 'trainingResult'
dirToWatch = currentDirectoryPath + '/tmp/toWatch'  # directory where the patients to be done are added: one empty file should be created named with the patient_id to be done
dirForRes = currentDirectoryPath + '/tmp/forRes'
pathTocTakes = currentDirectoryPath + '/tmp/'  # need to have 2 subdirectories: 'avant' and 'apres' that must be empty. cTakes should be runing from avant to apres.
version = '13-new-ctakes_new_Pat'
resultsDirectory = currentDirectoryPath + '/resultDir/'
pathToCCIs = currentDirectoryPath + "/Anonymized_CCI-CD_without_outliers_ALL.csv"
cTakesUrl = "http://localhost:9999/"

raxa_ctakes_url = "http://" + config.myIp + ":8080/openmrs/"
time_to_wait = 2  # in sec


if __name__ == '__main__':
    splittingPOD()



def create_todo_dataframe(patient_id, list_col_num):
    # Now getting the prediction for patient comming in restcall
    pat_todo = [patient_id]
    res_total = pd.DataFrame()

    print('Formatting Data of the Patient in required Format')
    todo = makeTodoRunServer(pat_todo)

    for pat in pat_todo:
        res_pat = []
        for_ctakes = []
        obs_ids_from_value_text = []
        # Select on the patient
        todo_pat = todo[
            (todo['patient_id'] == pat) & (todo['concept_id'] != '33334027')]  # TODO presenting complaints
        forPOD = todo_pat[todo_pat['value_text'].apply(
            lambda x: 'pod' in x[:10].lower() or ('plan' not in x.lower() and 'pod' in x[10:].lower()))]

        # if we have PODs
        if not forPOD.empty:
            # Get the POD as int
            forPOD['POD'] = forPOD['value_text'].apply(
                lambda x: re.search('\d+', x).group() if re.search('\d+', x) != None else -1).astype(int)
            # Get the surgery date from the PODs
            forPOD['surgeryDate'] = forPOD[['obs_datetime', 'POD']].apply(
                lambda x: x[0] + datetime.timedelta(days=-x[1]) if (x[1] != -1) else datetime.date.min, axis=1)

            # Get the surgery date from the POD0
            list_surgery_date = forPOD['obs_datetime'][forPOD['POD'] == 0].values.tolist()

            # Compare both and remove mistakes (2 surgeries with a delta in date < 3days)
            for i in np.unique(forPOD['surgeryDate']).tolist():
                addok = True
                for j in list_surgery_date:
                    if abs((i - j).days) <= 2:
                        addok = False
                if addok:
                    list_surgery_date.append(i)

            last = [None] * len(list_col_num)
            last_dat = datetime.date.min
            idC = 0
            for dat in list(np.unique(todo_pat['obs_datetime'])):  # sorted on the fly
                todoPatSel = todo_pat[(todo_pat['obs_datetime'] <= dat) & (todo_pat['obs_datetime'] > last_dat)]
                last_dat = dat

                # Get all the info we have up to this POD
                now = extract_value_from_valuetext(todoPatSel, last)

                # Export for cTakes :
                for_ctakes = for_ctakes + list(todoPatSel['value_text'][(todoPatSel['value_text'] != 'nan') & (
                        todoPatSel['value_text'] != 'None')].values)

                # We are only taking obs_ids for freeText
                obs_ids_from_value_text = obs_ids_from_value_text + list(todoPatSel['obs_id'][
                                                                             (todoPatSel[
                                                                                  'value_text'] != 'nan') & (
                                                                                     todoPatSel[
                                                                                         'value_text'] != 'None') & (
                                                                                     todoPatSel[
                                                                                         'concept_id'] == '160632')].values)

                if obs_ids_from_value_text:
                    print('Adding concept values from  Ctakes to dataFrame')
                    now = update_extracted_value(todoPatSel, now, obs_ids_from_value_text)

                # Get the POD of all new infos from the closest earlier surgery
                if last != now:  # and sum(x is None for x in now)<len(now)):
                    if not for_ctakes:
                        for_ctakes = ['NULL']

                    lres = fetch_mlob_for_obs_ids(obs_ids_from_value_text)

                    for_ctakes = []
                    obs_ids_from_value_text = []

                    surgDate = getCloseSurgery(dat, list_surgery_date)
                    pod = (dat - surgDate).days if (dat - surgDate).days < 10000 else 'max'
                    surg_date_all = getAllSurgery(dat, list_surgery_date)
                    min_pod = getMinPOD(dat, list_surgery_date)

                    res_cTakes = [lres]

                    res_pat.append([pat, surgDate, surg_date_all, min_pod, pod] + now + [str(idC)] + res_cTakes)
                    last = now
                    idC = idC + 1

        if res_pat:
            res_pat_df = pd.DataFrame(res_pat)
            res_pat_df.columns = ['patient_id', 'dateSurgery', 'surgeriesPOD', 'minPOD', 'POD'] + list_col_num + [
                'idC'] + ['rescTakes']
            res_total = pd.concat([res_total, res_pat_df], axis=0)

    return res_total


def convert_to_json(res):
    resultJSON = {}
    podArray = []
    resultJSON['PODArray'] = podArray

    for index, row in res.iterrows():
        podJSON = {}
        resultJSON['patientId'] = row['patient_id']
        podJSON['POD'] = row['POD']
        podJSON['pred'] = row['pred']
        podArray.append(podJSON)

    return jsonify(resultJSON)


def Num_last(x):
    # print("Num_last")
    print(x)
    if x is None or str(x) is 'nan':
        return [None, None]
    res = [None]
    try:
        for dat, n in x:
            if type(n) == float or type(n) == int or n.isdigit():
                res.append(float(n))
    except ValueError:
        print(x)
    return [res[-1], None]


def Num_last_series(x):
    print("Num_last")
    print(x)
    if x is None or str(x) is 'nan':
        return [None, None]
    res = [None]
    try:
        for dat, n in x.iteritems():
            if type(n) == float or type(n) == int or n.isdigit():
                res.append(float(n))
    except ValueError:
        print(x)
    return [res[-1], None]


def Num_mean(x):
    if x is None or str(x) is 'nan':
        return [None, None]
    res = []
    try:
        for dat, n in x:
            if type(n) == float or type(n) == int or n.isdigit():
                res.append(float(n))
    except ValueError:
        print(x)
    return [np.mean(res), np.std(res)]


def funOnBool_or(x):
    print(x)
    if x is None:
        return None
    res = False
    for dat, n in x:
        res = res or n
    return res * 1


def funOnBool_last(x):
    if x is None:
        return None
    res = False
    for dat, n in x:
        res = n
    return res * 1








