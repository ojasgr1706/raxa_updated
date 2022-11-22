import re


re_match_for_special_char = re.compile('[^A-Za-z0-9]+')


# For Update now list from mlob

listNumDic = {'weight': '0', 'height': '1', 'serum_sodium': '2',
              'serum_potassium': '3',
              'serum_creatinine': '4',
              'serum_calcium': '5',
              'hemoglobin': '6',
              'phosphorous test': '7',
              'wbc': '8',
              'urea': '9',
              'serum_albumin': '10',
              'direct_bilirubin': '11',
              'sgpt': '12',
              'bilirubin': '13',
              'sgot': '14',
              'total_protein': '15',
              'alkaline_phosphatase': '16',
              'serum_ggt': '17',
              'prothrombin': '18',
              'pulse': '25',
              'bp': '26',
              'urine output': '27',
              'urine': '28',
              'intake': '29',
              'temp': '30',
              'stoma': '31',
              'rt': '32',
              'spo2': '33'
              }

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



def extract_value_normalize_date(concept_value_str):
    concept_value = None
    concept_value_str = str(concept_value_str)

    if isinstance(concept_value_str, str):
        if concept_value_str is not None or str(concept_value_str) != 'nan':
            coefficient = normalize_coeffient_for_date(concept_value_str)
            numlist = re.findall('\d+', concept_value_str)
            if numlist and len(numlist) > 0:
                try:
                    concept_value = float(numlist[0])
                    concept_value = concept_value * coefficient
                except ValueError as ex:
                    print(ex)
    return concept_value


# Get Unique from list irrespective of Data
def unique(l):
    if l is None:
        return None
    res =[]
    for i in l:
        if i not in res:
            res.append(i)
    return res




def wrap_datetime(concept_value, single_todo_df):
    single_todo_df['ctakes_value'] = concept_value
    tmp = single_todo_df[['obs_datetime', 'ctakes_value']][single_todo_df['concept_id'] == '160632']

    if not tmp.empty:
        tmp['tot'] = tmp.apply(lambda x: (x[0], x[1]), axis=1)
        res = []
        res.append(tmp['tot'].tolist()[0])
        del tmp['tot']
        del single_todo_df['ctakes_value']
        return res
    else:
        return None


def update_concepts_from_mlob(result_list, old_now, single_todo_df):
    patient_id = list(single_todo_df['patient_id'][0:1])[0]

    result_list = unique(result_list)
    lres = []

    for result in result_list:
        concept_name = result[1]
        freetext_concept_value = result[2]
        datatype_id = result[4]

        if concept_name:
            key = concept_name.lower()
            if freetext_concept_value:
                if key in listNumDic:
                    column_index = int(listNumDic[key])

                    # Value not present at the Index then try to find from the free_text
                    if old_now[column_index] is None:
                        extracted_concept_value = extract_value_normalize_date(freetext_concept_value)

                        # if Concept DataType is N/A then skip them
                        if datatype_id == 4:
                            # print('Data type Id 4: for ', concept_name)
                            pass
                        else:
                            if key.lower() == 'rt':
                                pass
                            elif key.lower() == 'temp' and extracted_concept_value < 40.0:
                                # Change in Temperature in Fahrenheit
                                print("Temp in Celcius with value: " + str(extracted_concept_value) + " for patientId " + str(patient_id)
                                      + " at index: " + str(column_index))
                                extracted_concept_value = (extracted_concept_value * 1.8) + 32
                                old_now[column_index] = wrap_datetime(extracted_concept_value, single_todo_df)
                            else:
                                old_now[column_index] = wrap_datetime(extracted_concept_value, single_todo_df)
                    else:
                        pass
            else:
                if concept_name not in lres:
                    lres.append(concept_name)

    lres = remove_value_keywords(lres)

    return old_now, lres


def remove_value_keywords(ctakes_list):
    kvList = ['pulse', 'bp', 'urine output', 'urine', 'intake', 'temp', 'stoma', 'rt', 'spo2',
              'pod', 'weight', 'height', 'serum_sodium', 'serum_potassium', 'serum_creatinine', 'serum_calcium',
              'hemoglobin', 'phosphorous_test', 'wbc', 'urea', 'serum_albumin', 'direct_bilirubin', 'sgpt', 'bilirubin',
              'sgot',
              'total_protein', 'alkaline_phosphatase', 'serum_ggt', 'prothrombin', 'phosphorous test']
    res_list = []
    for keyword in ctakes_list:
        if keyword.lower() not in kvList:
            res_list.append(keyword)
    return res_list