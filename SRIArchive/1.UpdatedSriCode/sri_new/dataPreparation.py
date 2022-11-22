import os
from Util import *
from variables import *
from structure import add_structure_concepts_from_db, combined_ctakesResponse_for_obs_ids, \
    update_concepts_from_mlob, get_data_for_obs_ids
from extractvalue import extact_value_concepts_from_freetext, create_dataframe_from_dic
import time
from todoPreparation import create_todo_df
import resource


# Create todoDataFrame
def get_todo_df():
    if not os.path.isfile(resultsDirectory + 'todo_diabetes1.pckl'):
        print('Step 0 Making todo_sub.pckl')
        todo = create_todo_df(pathToCCIs)
        todo.to_pickle(resultsDirectory + 'todo_' + version + '.pckl')
        print('Saved the todo_diabetes1.pckl')
    else:
        df = pd.read_pickle(resultsDirectory + 'todo_diabetes1.pckl')


def print_time(start_time, name):
    time_elapsed = (time.clock() - start_time)
    print('Time for the ' + name + ' ' + str(time_elapsed))


def assign_diabetes_col(todo, final):
    temp = list(todo['patient_id'][(todo['concept_id'] == '119481')])
    diabetes_patient_list = []

    for i in temp:
        if i not in diabetes_patient_list:
            diabetes_patient_list.append(i)

    def assgin_new_concept(x):
        if x[0] in diabetes_patient_list:
            return 1
        else:
            return 0

    # Setting hasDiabetesMellitus 1 for those columns having concept_id 119481
    final['hasDiabetesMellitus'] = final[['patient_id', 'hasDiabetesMellitus']].apply(assgin_new_concept, axis=1)
    return final


# Create DataFrame from Ctakes Takes
def create_structure_df():
    print('Step 1: Reading tododiabetes.pickl')
    todo = pd.read_pickle(resultsDirectory + 'todo_diabetes1.pckl')
    print(todo.shape)

    print('RUNNING FOR SMALL OBSERVATIONS: ')
    # Using subset of todo DataFrame
    # todo = todo[0:50000]
    final = pd.DataFrame()
    final['patient_id'] = todo['patient_id'].drop_duplicates()
    final = final.reset_index(drop=True)
    final['hasDiabetesMellitus'] = 0

    # Adding hasDiabetesCol concept Column
    final = assign_diabetes_col(todo, final)

    print('Number of patients having Diabetes: ',
          final['hasDiabetesMellitus'][final['hasDiabetesMellitus'] == 1].shape[0])

    res_total_df = pd.DataFrame()
    total_patients = final['patient_id'].drop_duplicates().shape[0]
    print('Total Patient: ' + str(total_patients))

    time_start = time.clock()
    print('Step2:  Making todo frame from database and NLP')
    for patient_id in list(final['patient_id'].drop_duplicates().values):
        res_patient_list = []
        freetext_obs_ids = []

        # Select on the patient data
        todo_patient_df = todo[
            (todo['patient_id'] == patient_id) & (todo['concept_id'] != '33334027')]  # TODO presenting complaints

        ctakes_value_list = [None] * len(listColNum)

        # Contains values for column name given in listColNum
        now = add_structure_concepts_from_db(todo_patient_df, ctakes_value_list)

        # We are only taking obs_ids for freeText
        freetext_obs_ids = freetext_obs_ids + list(todo_patient_df['obs_id'][
                                                       (todo_patient_df[
                                                            'value_text'] != 'nan') & (
                                                               todo_patient_df[
                                                                   'value_text'] != 'None') & (
                                                               todo_patient_df[
                                                                   'concept_id'] == '160632')].values)

        if freetext_obs_ids:
            # print('Adding concept values from  ctakes to dataFrame for ' + patient_id)

            combine_res = get_data_for_obs_ids(freetext_obs_ids)
            combine_res_df = pd.DataFrame(combine_res)
            print(combine_res_df.shape)
            if combine_res_df.shape[1] == 5:
                combine_res_df.columns = ['obs_id', 'value_text', 'concept_value', 'concept_id', 'datatype_id']
                combine_res_df['obs_id'] = combine_res_df['obs_id'].astype(str)

                for obs_id in freetext_obs_ids:
                    row_series = combine_res_df[combine_res_df['obs_id'] == obs_id].values
                    row_list = row_series.tolist()

                    new_now, lres = update_concepts_from_mlob(row_list, now, todo_patient_df)

                    if now != new_now:
                        print('Old: ',now)
                        print('New: ', new_now)

                    if not lres:
                        lres = ['None']

                    res_ctakes = [lres]
                    res_patient_list.append(
                        [patient_id] + [obs_id] + new_now + res_ctakes)

                if res_patient_list:
                    res_patient_dataframe = pd.DataFrame(res_patient_list)
                    res_patient_dataframe.columns = ['patient_id'] + ['obs_id'] + listColNum + ['rescTakes']
                    res_total_df = pd.concat([res_total_df, res_patient_dataframe], axis=0)

    print_time(time_start, 'Structure text from db')

    return res_total_df, final, todo


def add_value_from_ctakes(res_total_df, final, todo):
    print('Adding concept values from  ctakes to dataFrame')
    for patient_id in list(final['patient_id'].drop_duplicates().values):

        freetext_obs_ids = []

        # Select on the patient
        todo_patient_df = todo[(todo['patient_id'] == patient_id) & (todo['concept_id'] != '33334027')]  # TODO presenting complaints

        # We are only taking obs_ids for freeText
        freetext_obs_ids = freetext_obs_ids + list(todo_patient_df['obs_id'][
                                                                     (todo_patient_df[
                                                                          'value_text'] != 'nan') & (
                                                                             todo_patient_df[
                                                                                 'value_text'] != 'None') & (
                                                                             todo_patient_df[
                                                                                 'concept_id'] == '160632')].values)

        freetext_obs_ids = unique(freetext_obs_ids)
        if freetext_obs_ids:
            combine_ctakes_response = combined_ctakesResponse_for_obs_ids(freetext_obs_ids)
            combine_res_df = pd.DataFrame(combine_ctakes_response)

            if combine_res_df.shape[1] == 2:
                combine_res_df.columns = ['ctakes_response', 'obs_id']
                combine_res_df['obs_id'] = combine_res_df['obs_id'].astype(str)

                for obs_id in freetext_obs_ids:
                    row_series = combine_res_df[combine_res_df['obs_id'] == obs_id].values
                    row_list = row_series.tolist()
                    # print('Row list: ', row_list)

                    if len(row_list) > 1:
                        ctakes_response = row_list[0][0]
                        key_dic, lres = extact_value_concepts_from_freetext(obs_id, ctakes_response)

                        if key_dic:
                            key_df = create_dataframe_from_dic(key_dic, patient_id, obs_id)

                        # if value occur in the freeText Append in the DataFrame
                        if not key_df.empty:
                            common_columns(key_df, res_total_df)
                            res_total_df = pd.merge(res_total_df, key_df,
                                                    left_on=['patient_id', 'obs_id'], right_on=['patient_id', 'obs_id'],
                                                    how='left')

                        # common_col = find_common_col(res_total_Df, key_df)
                        # res_total_Df = pd.merge(res_total_Df, key_df, how='outer', on=common_col)
                        # res_total_Df.dropna(how='all', inplace=True)

    return res_total_df


def merge_get_dummies(res_total_Df, final):
    res_total_Df['patient_id'] = res_total_Df['patient_id'].astype(str)
    res_total_Df['obs_id'] = res_total_Df['obs_id'].astype(str)
    res_total_Df = pd.merge(res_total_Df, final, how='left', on=['patient_id'])
    res_total_Df.to_pickle(resultsDirectory + version + '.pckl')
    dum = pd.get_dummies(res_total_Df['rescTakes'].apply(pd.Series).stack()).sum(level=0)

    # 1 ensures the res_total_Df Columns comes first
    # 0 dum columns comese first
    final = pd.concat([res_total_Df, dum], 1)

    print('Number of patients having Diabetes: ',
          final['hasDiabetesMellitus'][final['hasDiabetesMellitus'] == 1].drop_duplicates().shape[0])

    final.to_csv(resultsDirectory + version + '-dummies.csv')
    final.to_pickle(resultsDirectory + version + '-dummies.pckl')


if __name__ == '__main__':

    # Creating DataFrame from Structured Concepts
    res_total_Df, final_df, todo_df = create_structure_df()
    save_df(res_total_Df, 'res_total_df')
    save_df(todo_df, 'todo_df')
    save_df(final_df, 'final_df')

    # res_total_Df = pd.read_pickle(resultsDirectory + 'res_total_df')
    # todo_df = pd.read_pickle(resultsDirectory + 'todo_df')
    # final_df = pd.read_pickle(resultsDirectory + 'final_df')

    # Creating DataFrame from the Ctakes
    res_total_Df = add_value_from_ctakes(res_total_Df, final_df, todo_df)
    save_df(res_total_Df, 'res_value_df')

    # Merging with the intiail DataFrame and get Dummies
    merge_get_dummies(res_total_Df, final_df)
