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
version = '13-new-ctakes_new_Pat'
pathTocTakes = currentDirectoryPath + '/tmp/'
resultsDirectory = currentDirectoryPath + '/resultDir/'
pathToCCIs = currentDirectoryPath + "/Anonymized_CCI-CD_without_outliers_ALL.csv"  # Anonymized_CCI-CD_without_outliers.csv"
funToMerge = lambda x: x  # Rmean  ##function to use to merge the lists in multiples measurement fields
pd.options.mode.chained_assignment = None  # default='warn' #avoiding slicing false positive warning

start_time = time.time()

####################################
print('version: ' + str(version))


def splittingPOD():
    dictToCD = pd.DataFrame(
        [('0', 0), ('I', 1), ('II', 2), ('IIIa', 3), ('IIIb', 4), ('IV a', 5), ('IV b', 6), ('IVa', 5), ('IVb', 6),
         ('V', 7)])
    dictToCD.columns = ['CD', 'CDN']

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

    if not os.path.isfile(resultsDirectory + 'todo_sub.pckl'):
        print("Step1: Making todo_sub.pckl")
        todo = add_concept_form_db(pathToCCIs)
        todo.to_pickle(resultsDirectory + 'todo' + version + '.pckl')
    else:
        print("Step1: Reading todo_sub.pckl")
        todo = pd.read_pickle(resultsDirectory + 'todo13-new-ctakes_new_Pat.pckl')

    final = todo[['Reg no.', 'patient_id']].drop_duplicates()
    final = final[~final['Reg no.'].isnull()]
    final = addCCI(final, pathToCCIs)
    final = pd.merge(final, dictToCD, how='left')
    final['CD'][final['CD'] == 'IV a '] = 'IV a'
    final['CD'][final['CD'] == 'IV a'] = 'IVa'
    final['CD'][final['CD'] == 'IV b'] = 'IVb'
    final['CD'][final['CD'] == 'V '] = 'V'
    final['ind'] = final['CDN'] > 3

    res_total_Df = pd.DataFrame()
    total_patients = final['patient_id'].drop_duplicates().shape[0]
    count = 0
    print('Total Patient: ' + str(total_patients))

    print('Step2:  Making todo frame from database and NLP')
    if True:
        for pat in list(final['patient_id'].drop_duplicates().values):
            count = count + 1
            if count % 10 == 0:
                print(int(count * 100 / total_patients))
            res_patient_list = []
            for_ctakes_list = []
            obs_ids_from_value_text = []

            # Select on the patient
            todo_patient_df = todo[
                (todo['patient_id'] == pat) & (todo['concept_id'] != '33334027')]  # TODO presenting complaints

            def extract_POD(x):
                return 'pod' in x[:10].lower() or ('plan' not in x.lower() and 'pod' in x[10:].lower())

            for_pod_df = todo_patient_df[todo_patient_df['value_text'].apply(extract_POD)]

            # If we have PODs
            if not for_pod_df.empty:
                # Get the POD as int
                for_pod_df['POD'] = for_pod_df['value_text'].apply(
                    lambda x: re.search('\d+', x).group() if re.search('\d+', x) != None else -1).astype(int)

                # Get the surgery date from the POD's
                for_pod_df['surgeryDate'] = for_pod_df[['obs_datetime', 'POD']].apply(
                    lambda x: x[0] + datetime.timedelta(days=-x[1]) if (x[1] != -1) else datetime.date.min, axis=1)

                # Get the surgery date from the POD 0
                listSurgeryDate = for_pod_df['obs_datetime'][for_pod_df['POD'] == 0].values.tolist()

                # Compare both and remove mistakes (2 surgeries with a delta in date < 3days)
                for i in np.unique(for_pod_df['surgeryDate']).tolist():
                    addok = True
                    for j in listSurgeryDate:
                        if abs((i - j).days) <= 2:
                            addok = False
                    if addok:
                        listSurgeryDate.append(i)

                last = [None] * len(listColNum)
                last_date_min = datetime.date.min
                idC = 0
                for dat in list(np.unique(todo_patient_df['obs_datetime'])):  # sorted on the fly
                    todo_single_patient_df = todo_patient_df[
                        (todo_patient_df['obs_datetime'] <= dat) & (todo_patient_df['obs_datetime'] > last_date_min)]
                    last_date_min = dat

                    ## Get all the info we have up to this POD
                    now = extract_concepts_db(todo_single_patient_df, last)
                    # print('Old Now')
                    # print(now)
                    # print()

                    ## Export for cTakes :
                    for_ctakes_list = for_ctakes_list + list(todo_single_patient_df['value_text'][
                                                                 (todo_single_patient_df['value_text'] != 'nan') & (
                                                                         todo_single_patient_df[
                                                                             'value_text'] != 'None')].values)
                    # print(for_ctakes_list)

                    # We are only taking obs_ids for freeText
                    obs_ids_from_value_text = obs_ids_from_value_text + list(todo_single_patient_df['obs_id'][
                                                                                 (todo_single_patient_df[
                                                                                      'value_text'] != 'nan') & (
                                                                                         todo_single_patient_df[
                                                                                             'value_text'] != 'None') & (
                                                                                             todo_single_patient_df[
                                                                                                 'concept_id'] == '160632')].values)
                    if obs_ids_from_value_text:
                        print('Adding concept values from  ctakes to dataFrame')
                        now = add_concepts_from_ctakes(todo_single_patient_df, now, obs_ids_from_value_text)
                        # print('New Now: ')
                        # print(now)
                        # print()

                    # last == now when both are [None .... None]
                    # Get the POD of all new infos from the closest earlier surgery
                    if last != now:  # and sum(x is None for x in now)<len(now)):
                        if not for_ctakes_list:
                            for_ctakes_list = ['NULL']

                        # Fetching  the structured  text from the list
                        lres = fetch_mlob_for_obs_ids(obs_ids_from_value_text)

                        # Fetching the  structured text from the ctakes
                        # lres = fetch_mlob_using_ctakes(for_ctakes_list)

                        if not lres:
                            lres = ['None']

                        # Resetting the Values
                        for_ctakes_list = []
                        obs_ids_from_value_text = []

                        surgDate = getCloseSurgery(dat, listSurgeryDate)
                        pod = (dat - surgDate).days if (dat - surgDate).days < 10000 else 'max'
                        surgDateAll = getAllSurgery(dat, listSurgeryDate)
                        minPOD = getMinPOD(dat, listSurgeryDate)

                        res_cTakes = [lres]
                        res_patient_list.append(
                            [pat, surgDate, surgDateAll, minPOD, pod] + now + [str(idC)] + res_cTakes)
                        last = now
                        idC = idC + 1

            if res_patient_list:
                res_patient_dataframe = pd.DataFrame(res_patient_list)
                res_patient_dataframe.columns = ['patient_id', 'dateSurgery', 'surgeriesPOD', 'minPOD',
                                                 'POD'] + listColNum + ['idC'] + ['rescTakes']
                res_total_Df = pd.concat([res_total_Df, res_patient_dataframe], axis=0)

        print('Step3: res_total_df DataFrame completed not saving it in 13-new-ctakes_new_Pat.pckl')

        # del res_total_Df['idC']  # idC Column is use to fetch the freeText on basis of POD's (8595,0)

        print("Step4: Now merging the columns and adding it in 13-new-ctakes_new_Pat-dummies.pckl ")
        res_total_Df['patient_id'] = res_total_Df['patient_id'].astype(str)
        res_total_Df = pd.merge(res_total_Df, final, how='left', on=['patient_id'])
        res_total_Df.to_pickle(resultsDirectory + version + '.pckl')
        dum = pd.get_dummies(res_total_Df['rescTakes'].apply(pd.Series).stack()).sum(level=0)
        final = pd.concat([res_total_Df, dum], 1)
        final.to_csv(resultsDirectory + version + '-dummies.csv')
        final.to_pickle(resultsDirectory + version + '-dummies.pckl')
    else:
        print('Not making 13-new-ctakes_new_Pat.pckl')
        res_total_Df = pd.read_pickle(resultsDirectory + version + '.pckl')

    res_total_Df


def fetch_mlob_using_ctakes(ctakesList):
    for text in ctakesList:
        response = ctakes_post_call(text)
        lres = get_extracted_concepts_new_ctakes(response)
        return lres


def delete_done(resTotal):
    aaa = list(resTotal[resTotal['idC'] == -1].index)
    for i in aaa:
        os.remove(pathTocTakes + 'avant/' + i)
