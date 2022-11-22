# --------------------------------------------------------------------------------
# SPADE - Support for Provenance Auditing in Distributed Environments.
# Copyright (C) 2015 SRI International
# This program is free software: you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
# --------------------------------------------------------------------------------


## this is the main file which will :
#   - create the todo dataframe building the base of the dataset (get all the lines from the obs table for the patients we want)
#       for that it will call makeTodo.py which requires access to the database
#           makeTodo will call KVExtractor.py
#   - create directories pathTocTakes for the use of cTakes, it just has to be an empty directory (it will delete everything that is inside)

#   - create the 'final' dataframe which sum up all the different patient and CD/CCI/CDN/ind (ind is indicator of CD>IIIa) this it to be changed if you integrate CD per POD
#   - run a for loop for all the patients
#       - gets all the different POD and surgery dates possible and then treats them to be sure of the surgery date <- this could be remove if the surgery date is introduced in the data and we are sure of it (for now there are a lot of mistake from the typers for a difference of one or two days around the real date)
#       - for each POD based on the real surgery date it will run runAll from onTodo.py
#               this will run the part of code from toolsTodoSummedUp.py to get all the little information and build a complete line of the final dataset
#       - everything is merge in the resTotal dataframe and output as pickle so that we can come back from here (the purpose of the if(True) is to run all the code to get resTotal. just change it in if(False) and resTotal will be get from the back up and a lot of time will be saved)
#   - For each encounter a file is created in pathTocTakes and an ID is created in idC (one per patient.encounter) just to get it back after cTakes runs

######## SECOND PART

# CAUTION!!!!! please check that in the pathTocTakes/avant/ directory there is no file of size 0Ko which would cause cTakes to crash (I couldnt manage to treat that efficiently)


# run cTakes as the output of the code tells you to.
# the second part can be run at the same time as cTakes, this
# it is just mean to get back all the information from cTakes thanks to forcTakes.py file and put it at the right place in the dataframe thanks to the idC
# the output of cTakes is gathered in a list of all the terms
# this is output in the resultDir directory, then a bag of word is done out of that thanks to the function get_dummies() of pandas
# it is output as -dummies and then you go the prediction_splitted.py to get back this dataset and play with the prediction/assignment algorithm.

##ouput :
## csv : resultsDirectory+version+'.csv'
## pickle : resultsDirectory+version+'.pckl'


from makeTodo import *

from onTodo import *
from forcTakes import *
import re
import os.path
import os
import shutil
import time

currentDirectoryPath = os.path.dirname(os.path.abspath(__file__))
version = 'diabetes'
pathTocTakes = currentDirectoryPath + '/tmp/'
resultsDirectory = currentDirectoryPath + '/resultDir/'
pathToCCIs = currentDirectoryPath + "/Anonymized_CCI-CD_without_outliers_ALL.csv"  # Anonymized_CCI-CD_without_outliers.csv"
funToMerge = lambda x: x  # Rmean  ##function to use to merge the lists in multiples measurement fields
pd.options.mode.chained_assignment = None  # default='warn' #avoiding slicing false positive warning

start_time = time.time()

####################################
print('version: ' + str(version))

listColNum = ['weight',
              'Height',
              'SERUM_SODIUM',
              'SERUM_POTASSIUM',
              'SERUM_CREATININE',
              'SERUM_CALCIUM',
              'hemoglobin',
              'Phosphorous Test',
              'BLOOD SUGAR',
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
              'Gestational diabetes',
              'pulse', 'bp', 'urine output', 'urine',
              'intake', 'temp', 'stoma', 'rt', 'spo2',
              'PLT',
              'words']


# Main Function for preparing the Data Frame from Training
def prepare_data_frame():
    # STEP 0: if todo_df data frame not exist then build it from database
    if not os.path.isfile(resultsDirectory + 'tododiabetes.pckl'):
        print("Step1: Making todo_sub.pckl")
        todo = add_concept_form_db(pathToCCIs)
        todo.to_pickle(resultsDirectory + 'todo' + version + '.pckl')
        print('Saved the new todo.pckl')
    else:
        print("Step1: Reading tododiabetes.pckl")
        todo = pd.read_pickle(resultsDirectory + 'tododiabetes.pckl')
        print(todo.shape)

    # STEP 1: Create a FINAL DATA FRAME  with "hasDiabetesMellitus" Column for Labeled Class
    # todo = todo[0:3000]
    final = create_final_dataframe(todo)
    print('Number of patients having Diabetes: ',
          final['hasDiabetesMellitus'][final['hasDiabetesMellitus'] == 1].shape[0])
    print('Total Patient: ' + str(final['patient_id'].drop_duplicates().shape[0]))

    # STEP 2: Create a res_total_df with values column and resCTakes column containing the ctakes Values
    res_total_df = add_resctakes(final, todo)

    # STEP 3: Now adding the values from Ctakes need to be improved
    res_total_df = add_ctakes_value(final, todo, res_total_df)

    # STEP 4: Now merging the columns and adding it in 13-new-ctakes_new_Pat-dummies.pckl
    res_total_df['patient_id'] = res_total_df['patient_id'].astype(str)
    res_total_df = pd.merge(res_total_df, final, how='left', on=['patient_id'])

    # STEP 5: Saving result in diabetes-dummies.pckl
    res_total_df.to_pickle(resultsDirectory + version + '.pckl')
    dum = pd.get_dummies(res_total_df['rescTakes'].apply(pd.Series).stack()).sum(level=0)

    # 1 ensures the res_total_Df Columns comes first  0 dum columns comes first
    final = pd.concat([res_total_df, dum],1)
    final.to_pickle(resultsDirectory + version + '-dummies.pckl')


# Create a intermediatory final DataFrame  with hasDiabetesMellitus Column for Labelled Class
def create_final_dataframe(todo):
    final = pd.DataFrame()
    final['patient_id'] = todo['patient_id'].drop_duplicates()
    final = final.reset_index(drop=True)
    final['hasDiabetesMellitus'] = 0

    # Adding new concept Column
    diabetes_patient_list = unique(list(todo['patient_id'][(todo['concept_id'] == '119481')]))

    def assgin_new_concept(x):
        # print(x[0], x[1])
        if x[0] in diabetes_patient_list:
            return 1
        else:
            return 0

    # Setting hasDiabetesMellitus 1 for those columns having concept_id 119481
    final['hasDiabetesMellitus'] = final[['patient_id', 'hasDiabetesMellitus']].apply(assgin_new_concept, axis=1)
    return final


# Add value from Ctakes to the respective Columns
def add_ctakes_value(final, todo, res_total_df):
    for patient_id in list(final['patient_id'].drop_duplicates().values):
        freetext_obs_ids = []

        # Select on the patient
        todo_patient_df = todo[
            (todo['patient_id'] == patient_id) & (todo['concept_id'] != '33334027')]  # TODO presenting complaints

        # We are only taking obs_ids for freeText
        freetext_obs_ids = freetext_obs_ids + list(todo_patient_df['obs_id'][
                                                       (todo_patient_df[
                                                            'value_text'] != 'nan') & (
                                                               todo_patient_df[
                                                                   'value_text'] != 'None') & (
                                                               todo_patient_df[
                                                                   'concept_id'] == '160632')].values)
        if freetext_obs_ids:
            print('Adding concept values from  ctakes to dataFrame')
            key_dic, lres = fetch_concepts_from_freetext(freetext_obs_ids)

            if key_dic:
                key_df = create_dataframe_fromDic(key_dic, patient_id)

            # if value occur in the freeText Append in the DataFrame
            if not key_df.empty:
                res_total_df = merge_dataframes(res_total_df, key_df)

    # common_col = find_common_col(res_total_Df, key_df)
    # res_total_Df = pd.merge(res_total_Df, key_df, how='outer', on=common_col)
    # res_total_Df.dropna(how='all', inplace=True)
    return res_total_df


# Add resCTakes Column in the res_total_df
def add_resctakes(final, todo):
    res_total_df = pd.DataFrame()

    for patient_id in list(final['patient_id'].drop_duplicates().values):
        res_patient_list = []
        for_ctakes_list = []
        freetext_obs_ids = []

        # Select one the patient data
        todo_patient_df = todo[
            (todo['patient_id'] == patient_id) & (todo['concept_id'] != '33334027')]  # TODO presenting complaints

        ctakes_value_list = [None] * len(listColNum)

        # Contains values for column name given in listColNum
        now = extract_value_from_valuetext(todo_patient_df, ctakes_value_list)

        # for_ctakes_list = for_ctakes_list + list(todo_patient_df['value_text'][
        #                                              (todo_patient_df['value_text'] != 'nan') & (
        #                                                      todo_patient_df[
        #                                                          'value_text'] != 'None')].values)

        # We are only taking obs_ids for freeText
        freetext_obs_ids = freetext_obs_ids + list(todo_patient_df['obs_id'][
                                                       (todo_patient_df[
                                                            'value_text'] != 'nan') & (
                                                               todo_patient_df[
                                                                   'value_text'] != 'None') & (
                                                               todo_patient_df[
                                                                   'concept_id'] == '160632')].values)

        if freetext_obs_ids:
            print('Adding concept values from  ctakes to dataFrame')

            # Only updating the values entered by extract_value Function
            now = update_extracted_value(todo_patient_df, now, freetext_obs_ids)

            lres = fetch_mlob_for_obs_ids(freetext_obs_ids)

            if not lres:
                lres = ['None']

            res_cTakes = [lres]
            res_patient_list.append([patient_id] + now + res_cTakes)
            ctakes_value_list = now

        if res_patient_list:
            res_patient_dataframe = pd.DataFrame(res_patient_list)
            res_patient_dataframe.columns = ['patient_id'] + listColNum + ['rescTakes']
            res_total_df = pd.concat([res_total_df, res_patient_dataframe], axis=0)

    print('Saving restotal_Df pckl')
    res_total_df.to_pickle('res_total_Df.pckl')
    return res_total_df


# Merge the two dataFrame by merging the common column into a single column with list
def merge_dataframes(res_total_df, key_df):
    # res_total_df.set_index('patient_id', inplace=True)
    # key_df.set_index('patient_id', inplace=True)

    res_df = pd.merge(res_total_df, key_df, left_index=True, right_index=True,
                      how='left', on="patient_id", suffixes=(' - 1', ' - 2'))

    for col in (c for c in res_total_df.columns if c in key_df.columns):
        if col != 'patient_id':
            # zip their values
            res_df[col] = list(zip(res_df[col + ' - 1'], res_df[col + ' - 2']))
            # remove the original columns
            del res_df[col + ' - 1'], res_df[col + ' - 2']

    return res_df


# Finding the Common Column in the two DataFrame
def find_common_col(res_total_df, key_df):
    common_list = ['patient_id']
    for i in res_total_df.columns:
        if i in key_df.columns:
            if i not in common_list:
                common_list.append(i)

    return common_list


# Create a Dataframe from the Dictionary
def create_dataframe_fromDic(dic, patient_id):
    values = [v for key, v in dic.items()]
    values.insert(0, patient_id)
    dataFrame_columns = [key for key, v in dic.items()]
    dataFrame_columns.insert(0, 'patient_id')
    todo = pd.DataFrame(values).transpose()
    todo.columns = dataFrame_columns
    return todo


# Fetch the mlob from Ctakes Rest Call
def fetch_mlob_using_ctakes(ctakesList):
    for text in ctakesList:
        response = ctakes_post_call(text)
        lres = get_extracted_concepts_new_ctakes(response)
        return lres


def fetch_mlob_using_ctakes(ctakesList):
    for text in ctakesList:
        response = ctakes_post_call(text)
        lres = get_extracted_concepts_new_ctakes(response)
        return lres


# Not used now
def delete_done(resTotal):
    aaa = list(resTotal[resTotal['idC'] == -1].index)
    for i in aaa:
        os.remove(pathTocTakes + 'avant/' + i)
