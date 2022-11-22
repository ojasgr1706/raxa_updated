import pandas as pd
from Util import unique
import numpy as np

def cd_dictionary(path=None):
    if (path == None):
        # hypertension = Amlovas
        return {
            'I': ['infection', 'unstable', 'bleed', 'critic',
                  'antiemeti', 'antipyreti', 'analgeti', 'diureti',
                  'dvt',
                  ['wound', 'infect'], 'atrial', ['mental', 'confusi'],  # samrat
                  'diarrh', 'unstable', ' aki',  # samrat
                  'af ', 'atelectasis', ['transient', 'confusion']  ##table from samrat
                  ],
            'II': [' transfus', 'prbc',
                   'hemorrha', 'haemorrha',
                   'antibio', 'zosyn', 'octreotid', 'colistin',
                   # samrat
                   ['chest', 'infect'], ['ryles', 'tube', 'insert'], ['ischemic', 'attack'],
                   ['infectious', 'diarrh'], ['urinar', 'tract', 'infect'],
                   # 'ivf'#intravenous fluid
                   'tachyarrhy', 'pneumoni', 'tia ', ' tia', ['anti', 'coagul'], ['diarrh', 'antibio'], 'uti',
                   ['nb', 'tube']  ##table from samrat
                   ],
            'IIIa': ['intervent',
                     'usg', 'ncct',  # exams
                     # samrat
                     'pacemaker', 'anaesthesi',  # local VS general
                     'bradyarrhythmi', 'anest', 'biloma', ['percutaneous', 'drain'], ['dehiscen', 'wound']
                     ##table from samrat
                     ],
            'IIIb': [' redo', 'redo ', 'rexplor',
                     ['redo', 'laparoto'], ['rexplo', 'laparato'], 'eventrat'  ##table from samrat
                     ],
            'IVa': [' dialysis ', ' hemorrhage ', ' stroke ', 'failure', 'perforation',
                    'caridac arrest', ['cardiac', ' mi'],
                    'digoxin',  # medicine again heart failure
                    'necro',
                    ['bowel', 'reconn'],
                    ' icu ', ' icu',
                    ['nasal', 'prong'],
                    'ischemic'  ##table from samrat
                    ],
            'IVb': ['cmv',
                    'mods'  ##table from samrat
                    ],
            'V': ['death', 'duef', 'died', 'expire', 'dead']
        }

    else:
        f = open(path, 'r')
        f.readline()
        d = {}
        for line in f.readlines():
            cas = line.split(',')
            listres = []
            for word in cas[1:]:
                word = word.replace('\n', '')
                if (len(word) > 0):
                    if (';' in word):
                        listres.append(word.split(';'))
                    else:
                        listres.append(word)
            d[cas[0]] = listres
        f.close()
        return d


dictToCD = pd.DataFrame(
    [('0', 0), ('I', 1), ('II', 2), ('IIIa', 3), ('IIIb', 4), ('IV a', 5), ('IV b', 6), ('IVa', 5), ('IVb', 6),
     ('V', 7)])
dictToCD.columns = ['CD', 'CDN']

## dictionnary for CD
listDic = cd_dictionary()

dicConcept = {
    'I': ['33334442'  # 'Has DVT prophylaxis been administered'
          ],
    'II': ['33334440'  # 'Has antibiotic prophylaxis been given within the last 60 minutes?'
           ],
    'IIIa': [],
    'IIIb': [],
    'IVa': [],
    'IVb': [],
    'V': [],
}


def assing_maxcd_number(todoPat, onId = ['160632', '170250000']):
    totalRes =[]
    maxGrade = '0'
    todoSel = todoPat[(todoPat['concept_id'].apply(lambda x: x in onId)) & (str(todoPat['value_text']) != 'nan')]
    todoSel = todoSel[['obs_datetime', 'value_text', 'concept_id']]

    if todoSel.shape[0] > 0:
        res = []
        # To find words and grade
        for grade, listWord in listDic.items():
            listConcept = dicConcept[grade]
            for i in listConcept:
                if i in list(todoSel['concept_id']):
                    res = res + [(grade, i, None)]
                    maxGrade = grade

            for i in listWord:
                resTmp = []
                # for each word
                if type(i) == str:
                    resTmp = list(todoSel['obs_datetime'][todoSel['value_text'].apply(lambda x: i in x.lower())].values)
                # for each list
                elif type(i) == list:
                    somme = todoSel['value_text'].apply(lambda x: i[0] in x.lower())
                    for j in i[1:]:
                        somme = np.logical_and(somme, todoSel['value_text'].apply(lambda x: j in x.lower()))
                    resTmp = list(todoSel['obs_datetime'][somme].values)

                ## TODO   ATTENTION AUX DATES
                if len(resTmp) > 0:
                    res = res + [(grade, i, unique(resTmp))]
                    maxGrade = grade

        totalRes.append([maxGrade, res])

    totalRes = pd.DataFrame(totalRes)
    if (totalRes.empty == True):
        return [None, None, None]
    totalRes.columns = ['maxCD', 'words']
    dictToCDMax = dictToCD
    dictToCDMax.columns = ['maxCD', 'maxCDN']
    finCSV = pd.merge(totalRes, dictToCDMax, how='left')
    return finCSV[['words', 'maxCD', 'maxCDN']].values.tolist()[0]  # TODO: PKKKKK ??????