from csv2arff import *
from classifier import *
from sklearn.impute import SimpleImputer
from sklearn.model_selection import StratifiedKFold
import numpy as np
import os
from plot_forest_importances import *
import pandas as pd
import pickle

currentDirectoryPath = os.path.dirname(os.path.abspath(__file__))
pathToPOD = currentDirectoryPath + '/resultDir/'
version = '13-new-ctakes_new_Pat'
n_iter = 1

# allInGlobal = None


listCol = ['weight',
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

listFeatures = [  # 'newCDN',
    'POD']
listExtracted = ['pulse',  # 'bp',
                 'urine output', 'urine', 'intake', 'temp', 'stoma', 'rt', 'spo2']

listWords = ['words']


def Num_last(x):
    print('Num Last')
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


###################################


def predictionSplittedTraining():
    # final = pd.read_pickle(pathToPOD+version+'-dummies.pckl')
    # final = pd.read_pickle(pathToPOD+'hybrid_new_sub.pckl')
    final = pd.read_pickle(pathToPOD + 'hybrid_new.pckl')

    if 'idC' in final:
        del final['idC']

    if 'ind' in final:
        del final['ind']

    for name in listNum + listExtracted:
        res = final[name].apply(Num_last)
        final[name] = res.apply(lambda x: x[0])

    for name in listBool:
        final[name] = final[name].apply(funOnBool_or)

    final = final[final['POD'] != 'max']
    final['POD'] = final['POD'].astype(int)

    c = 0
    d = 0
    toUse = final['patient_id'].copy()
    for pat in list(np.unique(final['patient_id'])):
        l = list(final['POD'][final['patient_id'] == pat])
        if sorted(l) == l:
            c = c + 1
            toUse[toUse == pat] = 1
        else:
            d = d + 1
            toUse[toUse == pat] = 0
    final = final[toUse == 1]

    final['hasDiabetesMellitus'] = final['hasDiabetesMellitus'].astype(int)

    del final['Is essential imaging displayed']
    del final['Has the imaging been discussed with radiologist preoperatively']
    svg = final.copy()

    toSample = svg[['patient_id', 'hasDiabetesMellitus']].groupby('patient_id').max().reset_index()
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

    allIn = pd.DataFrame()
    skf = StratifiedKFold(n_splits=10, shuffle=True, random_state=54)
    X_o = toSample['patient_id'].values
    y_o = toSample['hasDiabetesMellitus'].fillna(0).values

    for train_index, test_index in skf.split(X_o, y_o):
        print('o')
        X_train = X_o[train_index]
        Xtrain = svg[final['patient_id'].apply(lambda x: x in list(X_train))]
        Xtest = svg[final['patient_id'].apply(lambda x: x not in list(X_train))]
        finalForTest = final[final['patient_id'].apply(lambda x: x not in list(X_train))]

        x = Xtrain.drop(['hasDiabetesMellitus'], 1).values
        y = Xtrain['hasDiabetesMellitus'].values
        imp = SimpleImputer(missing_values='NaN', strategy='median', axis=0)
        imp = imp.fit(x)
        x = imp.transform(x)
        clf = MyClf()
        clf.train(x, y)

        X = Xtest.drop(['hasDiabetesMellitus'], 1).values
        X = imp.transform(X)
        res = finalForTest[['patient_id', 'hasDiabetesMellitus', 'POD', 'maxCDN', 'CD']]
        res['predDiabetes'] = clf.predict(X)
        allIn = pd.concat([allIn, res])

    # print('Saving the model')
    # pickle.dump(clf, open('finalize_model.pckl', 'wb'))

    FN = allIn[(allIn['predDiabetes'] == 0) & (allIn['hasDiabetesMellitus'] == 1)].shape[0]
    FP = allIn[(allIn['predDiabetes'] == 1) & (allIn['hasDiabetesMellitus'] == 0)].shape[0]
    print('FN : ' + str(FN))
    print('FP : ' + str(FP))

    TP = allIn[(allIn['predDiabetes'] == 1) & (allIn['hasDiabetesMellitus'] == 1)].shape[0]
    TN = allIn[(allIn['predDiabetes'] == 0) & (allIn['hasDiabetesMellitus'] == 0)].shape[0]
    print('TP : ' + str(TP))
    print('TN : ' + str(TN))

    recall = TP / (TP + FN)
    ratio_sensitivity = str(sum(allIn['predDiabetes'][allIn['hasDiabetesMellitus'] == 1])) + ' / ' + str(
        allIn[allIn['hasDiabetesMellitus'] == 1].shape[0])
    print('Recall/Sensitivity (A measure of a classifiers completeness): ' + str(recall) + ' ' + ratio_sensitivity)

    prec = TP / (TP + FP)
    F1 = 2 * recall * prec / (recall + prec)

    print('Precision (A measure of a classifiers exactness): ' + str(prec))
    print('F1: (A weighted average of precision and recall) : ' + str(F1))
    print('acc : ' + str(sum(allIn['predDiabetes'] == allIn['hasDiabetesMellitus']) / allIn.shape[0]))

    # allIn['roundPred'] = (allIn['pred'] > 0.3) * 1
    # print('acc : ' + str(sum(allIn['roundPred'] == allIn['ind']) / allIn.shape[0]))
    # print('sentivity : ' + str(sum(allIn['roundPred'][allIn['ind'] == 1])) + ' / ' + str(
    #     allIn[allIn['ind'] == 1].shape[0]))
    # print('FP : ' + str(allIn[(allIn['roundPred'] == 1) & (allIn['ind'] == 0)].shape[0]))
    # print('FN : ' + str(allIn[(allIn['roundPred'] == 0) & (allIn['ind'] == 1)].shape[0]))
    #
    # print('mat')
    # print(str(allIn[(allIn['roundPred'] == 0) & (allIn['ind'] == 0)].shape[0]) + '      ' + str(
    #     allIn[(allIn['roundPred'] == 1) & (allIn['ind'] == 0)].shape[0]))
    # print(str(allIn[(allIn['roundPred'] == 0) & (allIn['ind'] == 1)].shape[0]) + '      ' + str(
    #     allIn[(allIn['roundPred'] == 1) & (allIn['ind'] == 1)].shape[0]))

    acc = []
    dis = []
    fp = []
    fn = []
    x = []

    return allIn


def predictionSplittedPrediction(patientId, traningResult):
    diff = []
    resultJSON = {}
    podArray = []

    resultJSON['patientId'] = patientId
    allInGlobal = traningResult

    forPat = allInGlobal[allInGlobal['patient_id'] == patientId]

    resultJSON['PODArray'] = podArray

    minRaxa = forPat[['POD', 'maxCDN']].groupby('maxCDN').min().reset_index()
    if (minRaxa[minRaxa['maxCDN'] >= 3].shape[0] > 0):
        minRaxa = min(minRaxa[minRaxa['maxCDN'] >= 3]['POD'])
    else:
        minRaxa = 1000
    forPat['MYCDN'] = (forPat['hasDiabetesMellitus'] > 0.4) * 1
    minMY = forPat[['POD', 'MYCDN']].groupby('MYCDN').min().reset_index()
    if (minMY[minMY['MYCDN'] >= 3].shape[0] > 0):
        minMY = min(minMY[minMY['MYCDN'] >= 3]['POD'])
    else:
        minMY = 1000

    diff.append(minRaxa - minMY)

    if (9999999 in list(forPat['POD'])):
        forPat['POD'][forPat['POD'] == 9999999] = max(forPat['POD'][forPat['POD'] != 9999999]) + 1

    for patient_id, pod, ind, cd, maxCdn, pred, roundPred in list(
            forPat[['patient_id', 'POD', 'hasDiabetesMellitus', 'CD', 'maxCDN', 'pred', 'roundPred']][
                forPat['patient_id'] == patientId].values):
        podJSON = {}
        podJSON['POD'] = pod
        podJSON['hasDiabetesMellitus'] = ind
        podJSON['CD'] = cd
        podJSON['maxCDN'] = maxCdn
        podJSON['predDiabetes'] = pred
        podJSON['roundPred'] = roundPred
        podArray.append(podJSON)

    return resultJSON
