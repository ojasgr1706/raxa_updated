import pandas as pd
import re
import numpy as np
import os
# Merge sql query result into the DataFrame passed
currentDirectoryPath = os.path.dirname(os.path.abspath(__file__))
pathToCCIs = currentDirectoryPath + "/Anonymized_CCI-CD_without_outliers_ALL.csv"  # Anonymized_CCI-CD_without_outliers.csv"
resultsDirectory = currentDirectoryPath + '/resultDir/'
version = 'diabetes1'


re_match_for_special_char = re.compile('[^A-Za-z0-9]+')


def normalize_coeffient_for_date(string):
    # print(string, type(string))
    coefficent = 1
    if string.lower() != 'none':
        value_list = re_match_for_special_char.sub(' ', string).split()
        if len(value_list) > 1:
            value = value_list[1]
            value = value.lower()
            switcher = dict(weeks=7, week=7, month=30, months=30, years=365, year=365)
            coefficent = switcher.get(value, 1)
    return coefficent


def common_columns(df1, df2):
    common_columns_list = []
    for i in df1.columns:
        if i in df2.columns:
            if i not in common_columns_list:
                common_columns_list.append(i)

    print(common_columns_list)
    return common_columns_list


def extract_value_normalize_date(concept_value_str):
    concept_value = None
    concept_value_str = str(concept_value_str)

    if isinstance(concept_value_str, str):
        if concept_value_str is not None or str(concept_value_str) is not 'nan':
            coefficient = normalize_coeffient_for_date(concept_value_str)
            numlist = re.findall('\d+', concept_value_str)
            if numlist and len(numlist) > 0:
                try:
                    concept_value = float(numlist[0])
                    concept_value = concept_value * coefficient
                except ValueError as ex:
                    print(ex)
    return concept_value


def save_df(df, name):
    print('Saving' + resultsDirectory + name)
    df.to_pickle(resultsDirectory + name)


def merge_query_result_df(todo, query_result, lname, typ=int, dropna=False):
    res = pd.DataFrame(query_result)
    res.columns = lname
    res = pd.merge(todo, res, how='left')
    if dropna:
        res = res.dropna()
    if typ is not None:
        res[lname[-1]] = res[lname[-1]].astype(typ)
    return res


# Bind list with any type of Data
def bind_list(last, enc):
    res = []
    if last is None:
        res = enc
    elif enc is None:
        res = last
    elif type(last)!=list :
        res = [last,enc]
    elif type(enc)!=list :
        res = [last,enc]
    else:
        res = last + enc
    return unique(res)


# get all values from numerical fields
def get_value_from_num(todo, concept_id, fun):
    tmp = todo[['patient_id', 'value_numeric', 'obs_datetime']][todo['concept_id'] == concept_id]
    if not tmp.empty:
        tmp['value_numeric'] = tmp['value_numeric'].astype(float)
        tmp['tot'] = tmp.apply(lambda x : (x[2], x[1]), axis=1)
        tmp = tmp[['patient_id', 'tot']].groupby('patient_id')['tot'].apply(fun).reset_index()
        return tmp['tot'].values.tolist()[0]#TODO: CLEAN
    else:
        return None


# get all values from booleans fields
def get_value_from_bool(todo, concept_id, fun):
    tmp = todo[['patient_id', 'value_text', 'obs_datetime']][todo['concept_id'] == concept_id]
    if not tmp.empty:
        tmp['value_text'] = tmp['value_text'].apply(lambda x: 'yes' in x.lower()).astype(bool)
        tmp['tot'] = tmp.apply(lambda x: (x[2], x[1]), axis=1)
        tmp = tmp[['patient_id', 'tot']].groupby('patient_id')['tot'].apply(fun).reset_index()
        return tmp['tot'].values.tolist()[0]  # TODO: CLEAN
    else:
        return None


# get all values from kv etractor fields
def extract_key_value(todo, name, fun):
    tmp = todo[['patient_id', name, 'obs_datetime']][~todo[name].isnull()]
    if not tmp.empty:
        tmp['tot'] = tmp.apply(lambda x: (x[2], x[1]), axis=1)
        tmp = tmp[['patient_id', 'tot']].groupby('patient_id')['tot'].apply(fun).reset_index()
        return tmp['tot'].values.tolist()[0]  # TODO: CLEAN
    else:
        return None


# Get Unique from list irrespective of Data
def unique(l):
    if l is None:
        return None
    res =[]
    for i in l:
        if i not in res:
            res.append(i)
    return res


# get values from concept_id = 1026
def get_plt(todo):
    tmp = todo[['patient_id', 'value_text', 'obs_datetime']][todo['concept_id'] == '1026']
    if not tmp.empty:
        tmp['PLT'] = tmp['value_text'].apply(lambda x : re.search(r'\d+',x).group())
        tmp['tot'] = tmp.apply(lambda x : (x[2], x[3]), axis=1)
        tmp = tmp[['patient_id', 'tot']].groupby('patient_id')['tot'].apply(list).reset_index()
        return tmp['tot'].values.tolist()[0]#TODO: CLEAN
    else:
        return None


def runKVExtractor(directoryData, cursor, todo):
    query = ("SELECT obs_id, value_text, concept_id, obs_datetime, patient_id FROM obs, encounter where obs.encounter_id = encounter.encounter_id and concept_id=160632 and patient_id in (" +
                ','.join(list(np.unique(todo['patient_id']))) + ")")

    cursor.execute(query)

    lrow = []
    for row in cursor:
        if (type(row[1]) == str):  # select not None neither other than str
            # filename = directoryTest+"/"+str(row[0])+".txt"
            lrow.append([row[0], row[3], row[4], row[1]])

    ######################doing the splitting and extraction
    obs = {}
    wait = False
    last = ''
    ## preventing any html string to be solved
    lhtml = ['serif', 'font-family']  # string indicator of html fields

    res = []  ## list of dictionnary containing all the results

    for obs_id, obs_datetime, patient_id, row in lrow:
        # dic to store the results for this obs_id
        obs = {}

        ## Work on looking for the POD which is to be an integer in the 10 first characters after the term 'pod'
        if ("POD" in row.upper()):
            i = row.upper().find("POD")
            resPOD = re.search(r'\d+', row[i:i + 10])
            if (resPOD != None):
                obs['pod'] = int(resPOD.group())  ## if found add to the result <'pod', X> otherwise don t do nothing

        ## Split on all the possible character that can split tokens
        for lineTmp in row.split('\\n'):
            for line in lineTmp.split('\n'):
                for subtTmp in line.split('\t'):
                    for subt in subtTmp.split('\\t'):
                        for subv in subt.split(','):
                            for sub in subv.split(';'):

                                if (
                                not any(word in sub for word in lhtml)):  # if there is no sign of being an html line

                                    if (
                                    wait):  # if we are waiting for something then add it to the dictionnary with the correct key
                                        obs[last.strip().lower()] = sub
                                        wait = False  # stop waiting

                                    elif (len(
                                            sub) > 1 and ':' in sub):  # if there is ':' then we re looking for a measerement
                                        nb = sub.count(':')
                                        start = 0

                                        ## Still to be analysed
                                        if (nb % 2 == 0):  # is to be used for the kind of
                                            # "Assesment : BP : 120"
                                            lword = ['drains', 'assessment', 'objective']
                                        # if(not any(word in sub.lower() for word in lword)):
                                        # print("-----------"+sub)

                                        ssub = sub.split(':')
                                        last = ssub[0]
                                        if (ssub[1].replace(' ', '') == ''):
                                            wait = True
                                        else:
                                            obs[ssub[0].strip().lower()] = ssub[1].strip()
                                    elif ("POD" in sub.upper() or "POST OP DAY" in sub.upper()):
                                        obs[
                                            'pod'] = sub  # Just in case it remains and we did not found it with the 10 characters rule
        #                            else:
        #                                print(sub)
        if (obs != {}):
            trie = faireTri(obs_id, obs_datetime, patient_id, obs)
            if (trie != None):
                res.append(trie)

                ############# save in a csv file

    listParams = ['obs_id', 'obs_datetime', 'patient_id', 'pulse', 'bp', 'urine output', 'urine', 'intake', 'temp',
                  'stoma', 'rt', 'spo2']
    f = open(directoryData + "/extractionText.csv", "w")
    f.write(';'.join(listParams) + "\n")
    for d in res:
        toPrint = []
        for p in listParams:
            if (p in d.keys()):
                toPrint.append(str(d[p]))
            else:
                toPrint.append('')
        f.write(';'.join(toPrint) + "\n")
    f.close()
    print('Complete Extracting the Key Value Pairs')


############### function to extract infos
def faireTri(obs_id, obs_datetime, patient_id, obs):
    res = {}
    res['obs_id'] = obs_id
    res['obs_datetime'] = obs_datetime
    res['patient_id'] = patient_id
    key = "pulse"
    if (key in obs.keys() and obs[key] != ''):
        res[key] = int(re.search(r'\d+', obs[key]).group())

    key = "bp"
    if (key in obs.keys() and obs[key] != ''):
        if (len(re.findall(r'\d+\/\d+', obs[key])) > 0):
            res[key] = re.findall(r'\d+\/\d+', obs[key])[0]

    key = "urine output"
    if (key in obs.keys() and obs[key] != ''):
        if (len(re.findall(r'\d+', obs[key])) > 0):
            res[key] = re.findall(r'\d+', obs[key])[0]
        elif 'adequate' in obs[key].lower():
            res[key] = "adequate"
        elif 'nil' in obs[key].lower() or '-' in obs[key].lower():
            res[key] = "nil"

    key = "urine"
    if (key in obs.keys() and obs[key] != ''):
        if (len(re.findall(r'\d+', obs[key])) > 0):
            res[key] = re.findall(r'\d+', obs[key])[0]
        elif 'adequate' in obs[key].lower():
            res[key] = "adequate"
        elif 'nil' in obs[key].lower() or '-' in obs[key].lower():
            res[key] = "nil"

    key = "intake"
    if (key in obs.keys() and obs[key] != ''):
        res[key] = re.findall(r'\d+', obs[key])

    key = "temp"
    if (key in obs.keys() and obs[key] != ''):
        if (len(re.findall(r'\d+', obs[key])) > 0):
            res[key] = int(re.findall(r'\d+', obs[key])[0])
            if (res[key] < 50):
                res[key] = int(1.8 * res[key] + 32)

    key = "stoma"
    if (key in obs.keys() and obs[key] != ''):
        if (len(re.findall(r'\d+', obs[key])) > 0):
            res[key] = int(re.findall(r'\d+', obs[key])[0])
            if ('litre' in obs[key].lower()):
                res[key] = res[key] * 1000

    key = "rt"
    if (key in obs.keys() and obs[key] != ''):
        if (len(re.findall(r'\d+', obs[key])) > 0):
            res[key] = int(re.findall(r'\d+', obs[key])[0])
            if ('litre' in obs[key].lower()):
                res[key] = res[key] * 1000

    key = "spo2"
    if (key in obs.keys() and obs[key] != ''):
        if (len(re.findall(r'\d+', obs[key])) > 0):
            res[key] = int(re.findall(r'\d+', obs[key])[0])

    if (len(res) > 1):
        return res
    else:
        return None
