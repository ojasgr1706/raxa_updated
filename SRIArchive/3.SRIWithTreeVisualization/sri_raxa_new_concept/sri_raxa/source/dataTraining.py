from classifier import *
from sklearn.preprocessing import Imputer
from sklearn.model_selection import StratifiedKFold
import numpy as np
import os
from plot_forest_importances import *
import pandas as pd
import numpy as np
import pickle

currentDirectoryPath = os.path.dirname(os.path.abspath(__file__))
pathToPOD = currentDirectoryPath + '/resultDir/'

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
           'words']

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
           'PLT',
           'BLOOD SUGAR'
           ]

listBool = ['Does Patient Have A Difficult Airway/Aspiration Risk?',
            'Does Patient Have A Risk of 500ml blood loss(7ml/kg in children)?',
            'Has antibiotic prophylaxis been given within the last 60 minutes?',
            'Has DVT prophylaxis been administered']


listExtracted = ['pulse', 'urine output', 'urine', 'intake', 'temp', 'stoma', 'rt', 'spo2']

listWords = ['words']


def Num_last(x):
    if x is None or str(x) is 'nan' or isNaN(x):
        return [None, None]
    res = [None]
    try:
        print('x: ' ,x , ' type of x', type(x))
        for dat, n in x:
            if type(n) == float or type(n) == int or n.isdigit():
                res.append(float(n))
    except ValueError:
        print(x)
    return [res[-1], None]

def isNaN(num):
    return num != num

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
        print('x: ', x, ' type of x', type(x))
        for dat, n in x:
            if type(n) == float or type(n) == int or n.isdigit():
                res.append(float(n))
    except ValueError:
        print(x)
    return [np.mean(res), np.std(res)]


def funOnBool_or(x):
    if x is None or str(x) is 'nan' or isNaN(x):
        return None
    res = False
    try:
        print('x: ', x, ' type of x', type(x))
        for dat, n in x:
            res = res or n
    except ValueError:
        print(x)
    return res * 1


def funOnBool_last(x):
    if x is None:
        return None
    res = False
    for dat, n in x:
        res = n
    return res * 1


def predictionSplittedTraining():
    # final = pd.read_pickle(pathToPOD + 'diabetes-dummies_new.pckl')
    final = pd.read_pickle(pathToPOD + 'diabetes-dummies.pckl')

    # final = final.astype(object).replace(np.nan, 'None')

    if 'idC' in final:
        del final['idC']

    if 'ind' in final:
        del final['ind']

    print(final.shape)

    for name in listNum + listExtracted:
        print('name: ' + name)
        res = final[name].apply(Num_last)
        final[name] = res.apply(lambda x: x[0])


    for name in listBool:
        print('boolname: ' + name)
        final[name] = final[name].apply(funOnBool_or)

    final['hasDiabetesMellitus'] = final['hasDiabetesMellitus'].astype(int)

    del final['Is essential imaging displayed']
    del final['Has the imaging been discussed with radiologist preoperatively']
    svg = final.copy()

    toSample = svg[['patient_id', 'hasDiabetesMellitus']].groupby('patient_id').max().reset_index()
    del svg['rescTakes']
    del svg['bp']
    del svg['patient_id']
    del svg['words']

    allIn = pd.DataFrame()
    skf = StratifiedKFold(n_splits=10, shuffle=True, random_state=54)
    X_o = toSample['patient_id'].values
    y_o = toSample['hasDiabetesMellitus'].fillna(0).values
    counter = 1

    for train_index, test_index in skf.split(X_o, y_o):
        print('o')
        X_train = X_o[train_index]
        Xtrain = svg[final['patient_id'].apply(lambda x: x in list(X_train))]
        Xtest = svg[final['patient_id'].apply(lambda x: x not in list(X_train))]
        finalForTest = final[final['patient_id'].apply(lambda x: x not in list(X_train))]

        x = Xtrain.drop(['hasDiabetesMellitus'], 1).values
        y = Xtrain['hasDiabetesMellitus'].values
        imp = Imputer(missing_values='NaN', strategy='median', axis=0)
        imp = imp.fit(x)
        x = imp.transform(x)
        clf = MyClf()
        clf.train(x, y)

        X = Xtest.drop(['hasDiabetesMellitus'], 1).values
        X = imp.transform(X)
        res = finalForTest[['patient_id', 'hasDiabetesMellitus']]
        res['predDiabetes'] = clf.predict(X)

        # Generate the Graph Code
        # feature_list = get_features_list(Xtrain)
        # clf.export_decision_tree(feature_list, counter)
        # counter = counter + 1

        allIn = pd.concat([allIn, res])

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

    allIn.to_pickle(pathToPOD + 'diabetes-trainingresult.pckl')

    return allIn


# Not giving the desired results
def predictionSplittedTrainingCDN():
    final = pd.read_pickle(pathToPOD + 'diabetes_cdn_dummy.pckl')

    if 'idC' in final:
        del final['idC']

    if 'maxCDN' in final:
        del final['maxCDN']
        del final['newCDN']

    if 'ind' in final:
        del final['ind']

    for name in listNum + listExtracted:
        print('name: ' + name)
        res = final[name].apply(Num_last)
        final[name] = res.apply(lambda x: x[0])

    for name in listBool:
        print('boolname: ' + name)
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

    final['hasDiabetesMellitus'] = final['hasDiabetesMellitus'] * 1

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
    # del svg['newCDN']

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
        imp = Imputer(missing_values='NaN', strategy='median', axis=0)
        imp = imp.fit(x)
        x = imp.transform(x)
        clf = MyClf()
        clf.train(x, y)

        X = Xtest.drop(['hasDiabetesMellitus'], 1).values
        X = imp.transform(X)
        res = finalForTest[['patient_id', 'hasDiabetesMellitus', 'POD', 'CD']]
        res['predDiabetes'] = clf.predict(X)
        allIn = pd.concat([allIn, res])

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

    acc = []
    dis = []
    fp = []
    fn = []
    x = []

    return allIn


def get_features_list(Xtrain):
    feature_list = list(Xtrain.columns)
    removed_column_list = Xtrain.columns[Xtrain.isna().all()].tolist()

    for i in removed_column_list:
        if i in feature_list: feature_list.remove(i)

    feature_list.remove('hasDiabetesMellitus')
    return feature_list


if __name__ == '__main__':
    # predictionSplittedOriginal()
    predictionSplittedTraining()
