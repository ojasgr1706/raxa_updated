from makeTodo import *
import json


# Requires distinct obsIds
# Fetch the concepts from Ctakes Response
def create_listFromCtakesResponse(obs_id):
    if not obs_id:
        return []
    else:
        cnx = mysql.connector.connect(user=userdb, password=pwddb, host=hostdb, database=databasedb)
        cursor = cnx.cursor()
        sql_query = ('SELECT ctakes_response from mlob where ctakes_response is not null and ctakes_response !="" and obs_id =' + str(obs_id)
                    + ' and concept_id not in (33334452, 33334004, 33334003, 170250004, 170250005) limit 1')

        cursor.execute(sql_query)
        res = cursor.fetchall()

        res_obj = json.loads(res[0][0])

        parseMentionList = ['LabValueMentionList', 'ProcedureMentionList', 'SignSymptomMentionList' , 'DiseaseDisorderMentionList' , 'DrugNerMentionList']

        # print(res_obj)

        ctakes_key_value_list = [];

        for key in res_obj.keys():
            mention_list = res_obj[key]
            if key in parseMentionList:
                for i in mention_list:
                    try:
                        mention_obj = json.loads(i)
                        # print(mention_obj['name'], end=" ")
                        ctakes_word  = mention_obj['name']
                        ctakes_value = None
                        if 'value' in mention_obj and len(mention_obj['value']) > 0:
                            # print(mention_obj['value'])
                            ctakes_value = mention_obj['value']

                        ctakes_key_value_list.append((ctakes_word, ctakes_value))
                        print()
                    except ValueError:
                        print('Parse Error while parsing ', i)

        return ctakes_key_value_list




obsIds = [14010, 16813 ,16815 ,16819, 16869, 16879, 16926, 17007, 17013, 17021, 17027]


if __name__ == '__main__':
    # for i in obsIds:
    print(fetch_concepts_from_freetext(obsIds))