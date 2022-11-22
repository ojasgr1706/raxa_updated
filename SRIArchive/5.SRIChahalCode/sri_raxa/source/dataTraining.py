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
featuresDirectory = currentDirectoryPath + '/featuresDir/'

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


def remove_features_from_xtrain(Xtrain):
    feature_list = list(Xtrain.columns)
    removed_column_list = Xtrain.columns[Xtrain.isna().all()].tolist()

    Xtrain = Xtrain.dropna(axis=1, how='all')

    feature_list.remove('hasDiabetesMellitus')
    del Xtrain['hasDiabetesMellitus']

    print(len(feature_list))
    print(Xtrain.shape)
    return Xtrain

def remove_previous_files():
    folder = featuresDirectory
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            # Remove subDirectory
            # elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)


def save_features_df(feature_importances_df, count):
    feature_file_name = featuresDirectory + 'features' + str(count) + '.csv'
    print(feature_file_name)
    # remove_previous_files()

    feature_importances_df.to_csv(feature_file_name)
    pass


def predictionSplittedTraining():
    # final = pd.read_pickle(pathToPOD + 'diabetes-dummies_new.pckl')
    final = pd.read_pickle(pathToPOD + 'diabetes-dummies.pckl')

    # final = final[0:1000]

    # final = final.astype(object).replace(np.nan, 'None')

    if 'idC' in final:
        del final['idC']

    if 'ind' in final:
        del final['ind']

    print(final.shape)

    for feature_file_name in listNum + listExtracted:
        res = final[feature_file_name].apply(Num_last)
        final[feature_file_name] = res.apply(lambda x: x[0])

    for feature_file_name in listBool:
        final[feature_file_name] = final[feature_file_name].apply(funOnBool_or)

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

    count = 0
    for train_index, test_index in skf.split(X_o, y_o):
        print('Current Split', count)
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
        print('Res Shape ', res.shape, ' X Shape ', X.shape);
        res['predDiabetes'] = clf.predict(X)

        reduce_xtrain = remove_features_from_xtrain(Xtrain)
        feature_importances_df = clf.features_importance(reduce_xtrain)

        save_features_df(feature_importances_df, count)
        count = count + 1

        # feature_list = get_features_list(Xtrain)
        # clf.export_decision_tree(feature_list)

        allIn = pd.concat([allIn, res])

    allIn.to_pickle(pathToPOD + 'allIn.pckl')

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