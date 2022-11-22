import config as config
from os import listdir
from flask import Flask, request, jsonify
from prediction_splitted import *

from sklearn.impute import SimpleImputer
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

app = Flask(__name__)


# Index
@app.route('/', methods=['GET', 'POST'])
def index():
    return "Flask is running fast"


@app.route("/splittingPOD")
def splitting():
    splittingPOD()
    return "Splitting POD and extracting success"


# Not used now splittiongPOD is doing extraction process
@app.route("/extraction")
def extraction():
    dirList = os.listdir(pathTocTakes + "/avant")

    # resTotal = pd.read_pickle(resultsDirectory + version + '.pckl')
    resTotal = pd.read_pickle(resultsDirectory + '13-new-ctakes_new_sub_Pat.pckl')

    resTotal['rescTakes'] = None
    resTotal.index = resTotal[['patient_id', 'idC']].apply(lambda x: str(x[0]) + ',' + str(x[1]), 1)
    count = 0
    totalCount = resTotal.shape[0]
    for pat, idC in list(resTotal[['patient_id', 'idC']][resTotal['idC'] != -1].values):
        count = count + 1
        print(str(int(count * 100 / totalCount)) + "%")
        fileName = str(pat) + ',' + str(idC)
        file = open(pathTocTakes + "/avant" + "/" + fileName, "r", encoding="utf-8")
        fileOutput = file.read()
        try:
            print("Sending New Ctakes request")
            ctakes_text_tmp = fileOutput
            lines = ctakes_text_tmp.splitlines()
            ctakes_text = ','.join(lines)
            print(ctakes_text)
            response = ctakes_post_call(ctakes_text)
            lres = get_extracted_concepts_new_ctakes(response)
            if (lres == []):
                lres = ['None']
            resTotal.set_value(fileName, 'rescTakes', lres)
            print("fileName: " + fileName + "- success")
        except Exception as e:
            resTotal.set_value(fileName, 'rescTakes', ['None'])
            print(str(e))
            print("fileName: " + fileName + "- failure")

    del resTotal['idC']  # idC Column is use to fetch the freeText on basis of POD's (8595,0)

    todo = pd.read_pickle(resultsDirectory + 'todo' + version + '.pckl')
    final = todo[['Reg no.', 'patient_id']].drop_duplicates()
    final = final[~final['Reg no.'].isnull()]
    final = addCCI(final, pathToCCIs)
    final = pd.merge(final, dictToCD, how='left')
    final['CD'][final['CD'] == 'IV a '] = 'IV a'
    final['CD'][final['CD'] == 'IV a'] = 'IVa'
    final['CD'][final['CD'] == 'IV b'] = 'IVb'
    final['CD'][final['CD'] == 'V '] = 'V'
    final['ind'] = final['CDN'] > 3

    resTotal['patient_id'] = resTotal['patient_id'].astype(str)
    resTotal = pd.merge(resTotal, final, how='left', on=['patient_id'])

    resTotal.to_csv(resultsDirectory + version + '.csv', index=False, sep='\t')
    resTotal.to_pickle(resultsDirectory + version + '.pckl')

    resTotal = pd.read_pickle(resultsDirectory + version + '.pckl')
    print('resTotal sans dummies :' + resultsDirectory + version + '.csv')

    dum = pd.get_dummies(resTotal['rescTakes'].apply(pd.Series).stack()).sum(level=0)
    final = pd.concat([resTotal, dum], 1)

    final.to_csv(resultsDirectory + version + '-dummies.csv', index=False, sep='\t')
    final.to_pickle(resultsDirectory + version + '-dummies.pckl')
    final = pd.read_pickle(resultsDirectory + version + '-dummies.pckl')

    return "success"


@app.route("/training")
def training():
    traning_result = predictionSplittedTraining()
    # traning_result: object = predictionSplittedTraining()
    traning_result.to_pickle(resultsDirectory + trainingResult + '.pckl')
    return "success"


# Prediction Route for predicting the post operative day surviving probability of a patient
@app.route("/prediction")
def prediction():
    patientId = request.args.get("patientId")
    print(resultsDirectory + 'traningResult.pckl')
    trainingResult = pd.read_pickle(resultsDirectory + 'trainingResult.pckl')
    resultJSON = predictionSplittedPrediction(patientId, trainingResult)
    return jsonify(resultJSON)


# Route for predicting the POD for a Patients
@app.route("/predictionForPatient")
def prediction_for_patient():
    listColNum = ['weight',
                  'Height',
                  'SERUM_SODIUM',
                  'SERUM_POTASSIUM',
                  'SERUM_CREATININE',
                  'SERUM_CALCIUM',
                  'hemoglobin',
                  'Phosphorous Test',
                  'WBC',
                  'UREA',
                  'SERUM_ALBUMIN',
                  'DIRECT_BILIRUBIN',
                  'SGPT',
                  'BILIRUBIN',
                  'SGOT',
                  'TOTAL_PROTEIN',
                  'ALKALINE_PHOSPHATASE',
                  'SERUM_GGT',
                  'prothrombin',
                  'Does Patient Have A Difficult Airway/Aspiration Risk?',
                  'Does Patient Have A Risk of 500ml blood loss(7ml/kg in children)?',
                  'Has antibiotic prophylaxis been given within the last 60 minutes?',
                  'Has DVT prophylaxis been administered',
                  'Is essential imaging displayed',
                  'Has the imaging been discussed with radiologist preoperatively',

                  'pulse', 'bp', 'urine output', 'urine',
                  'intake', 'temp', 'stoma', 'rt', 'spo2',

                  'PLT',
                  'words',
                  'maxCD',
                  'maxCDN',
                  'newCD',
                  'newCDN']

    listNum = ['weight',
               'Height',
               'SERUM_SODIUM',
               'SERUM_POTASSIUM',
               'SERUM_CREATININE',
               'SERUM_CALCIUM',
               'hemoglobin',
               'Phosphorous Test',
               'WBC',
               'UREA',
               'SERUM_ALBUMIN',
               'DIRECT_BILIRUBIN',
               'SGPT',
               'BILIRUBIN',
               'SGOT',
               'TOTAL_PROTEIN',
               'ALKALINE_PHOSPHATASE',
               'SERUM_GGT',
               'prothrombin',
               'PLT']

    listBool = ['Does Patient Have A Difficult Airway/Aspiration Risk?',
                'Does Patient Have A Risk of 500ml blood loss(7ml/kg in children)?',
                'Has antibiotic prophylaxis been given within the last 60 minutes?',
                'Has DVT prophylaxis been administered']

    listExtracted = ['pulse',  # 'bp',
                     'urine output', 'urine', 'intake', 'temp', 'stoma', 'rt', 'spo2']

    # final = pd.read_pickle(pathToPOD + version + '-dummies.pckl')
    print('Loading the Hybrid dummies pickle')
    final = pd.read_pickle(pathToPOD + 'hybrid_new.pckl')



    # Removing the  ind  column responsible  for the cdn
    del final['ind']


    list_col_final = final.columns

    for name in listNum + listExtracted:
        res = final[name].apply(Num_last)
        final[name] = res.apply(lambda x: x[0])

    for name in listBool:
        final[name] = final[name].apply(funOnBool_or)

    final = final[final['POD'] != 'max']
    final['POD'] = final['POD'].astype(int)

    c = 0
    d = 0
    to_use = final['patient_id'].copy()

    # Get unique patientId and find the POD of each patient
    for pat in list(np.unique(final['patient_id'])):
        l_ist = list(final['POD'][final['patient_id'] == pat])
        if l_ist == sorted(l_ist):
            c = c + 1
            to_use[to_use == pat] = 1
        else:
            d = d + 1
            to_use[to_use == pat] = 0

    final = final[to_use == 1]
    final['hasDiabetesMellitus'] = final['hasDiabetesMellitus'] * 1

    del final['Is essential imaging displayed']
    del final['Has the imaging been discussed with radiologist preoperatively']
    svg = final.copy()

    to_sample = svg[['patient_id', 'hasDiabetesMellitus']].groupby('patient_id').max().reset_index()
    del svg['rescTakes']
    del svg['Reg no.']
    del svg['CD']
    del svg['CCI']
    del svg['CDN']
    del svg['surgeriesPOD']
    del svg['dateSurgery']
    del svg['patient_id']

    del svg['words']
    del svg['maxCD']
    del svg['newCD']
    del svg['newCDN']

    del svg['bp']

    # Get an equilibrated dataset
    svg = svg.reset_index()
    X_o = list(svg['index'][svg['hasDiabetesMellitus'] == 1]) + list(
        svg['index'][svg['hasDiabetesMellitus'] == 0].sample(svg['index'][svg['hasDiabetesMellitus'] == 1].shape[0]))
    Xtrain = svg[svg['index'].apply(lambda x: x in X_o)].drop('index', 1)

    # train the clf
    x = Xtrain.drop(['hasDiabetesMellitus'], 1).values
    y = Xtrain['hasDiabetesMellitus'].values
    imp = SimpleImputer(missing_values='NaN', strategy='median', axis=0)
    imp = imp.fit(x)
    x = imp.transform(x)
    clf = MyClf()
    clf.train(x, y)

    print('Clf Trained')


    patient_id = request.args.get("patientId")

    print('Creating res_total DataFrame')
    res_total = create_todo_dataframe(patient_id, listColNum)

    res_total.index = res_total[['patient_id', 'idC']].apply(lambda x: str(x[0]) + ',' + str(x[1]), 1)

    del res_total['idC']
    res_total['patient_id'] = res_total['patient_id'].astype(str)

    print('Making each value in the rescTakes column as pandas Series')
    dum = pd.get_dummies(res_total['rescTakes'].apply(pd.Series).stack()).sum(level=0)
    final = pd.concat([res_total, dum], 1)

    # add null columns and organize the same order
    for col in list_col_final:
        if col not in final.columns:
            final[col] = None
    final = final[list_col_final]

    if isinstance(final, pd.Series):
        for name in listNum + listExtracted:
            res = final[name].apply(Num_last_series)
            final[name] = res.apply(lambda x: x[0])
    else:
        for name in listNum + listExtracted:
            print(name)
            res = final[name].apply(Num_last)
            final[name] = res.apply(lambda x: x[0])

    for name in listBool:
        final[name] = final[name].apply(funOnBool_or)

    final = final[final['POD'] != 'max']
    final['POD'] = final['POD'].astype(int)
    final['hasDiabetesMellitus'] = final['hasDiabetesMellitus'] * 1

    del final['Is essential imaging displayed']
    del final['Has the imaging been discussed with radiologist preoperatively']
    svg = final.copy()

    del svg['rescTakes']
    del svg['Reg no.']
    del svg['CD']
    del svg['CCI']
    del svg['CDN']
    del svg['surgeriesPOD']
    del svg['dateSurgery']
    res = pd.DataFrame({'patient_id': svg['patient_id']})
    del svg['patient_id']
    del svg['words']
    del svg['maxCD']
    del svg['newCD']
    del svg['newCDN']

    del svg['bp']

    x = svg.drop(['hasDiabetesMellitus'], 1).values
    x = imp.transform(x)
    res['pred'] = list(clf.predict(x))
    res['POD'] = svg['POD']

    return convert_to_json(res)


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
    if x is None or str(x) == 'nan':
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
    if x is None or str(x) == 'nan':
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
    if x is None or str(x) == 'nan':
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


# app.debug = True
if __name__ == '__main__':
    app.run(host=config.myIp, port=5055)
