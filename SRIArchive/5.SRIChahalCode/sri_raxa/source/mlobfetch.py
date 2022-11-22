
import mysql.connector
import pandas as pd

userdb = 'root'
pwddb = 'qwas'
hostdb ='127.0.0.1'
databasedb = 'openmrsMLobs'




def fetch_mlob_for_obs_ids(obsIds):
    print(obsIds)
    cnx = mysql.connector.connect(user=userdb, password=pwddb, host=hostdb, database=databasedb)
    cursor = cnx.cursor()
    sql_query = ("SELECT value_text from mlob where obs_id in ("+','.join([str(f) for f in obsIds])+')')
    print(sql_query)
    cursor.execute(sql_query)
    res = cursor.fetchall()
    print(res)

def structure_freetext(obsIds):
    print(obsIds)
    cnx = mysql.connector.connect(user=userdb, password=pwddb, host=hostdb, database=databasedb)
    cursor = cnx.cursor()
    sql_query = ("SELECT obs_id, value_text from obs where obs_id in ("+','.join([str(f) for f in obsIds])+')')
    print(sql_query)
    cursor.execute(sql_query)
    res_df = pd.DataFrame(cursor.fetchall())
    res_df.columns = ['obs_id', 'value_text']
    res_df.dropna(inplace=True)
    res_df.reset_index(inplace=True, drop=True)
    print(res_df)


if __name__ == '__main__':
    list = ['222771', '222772', '222773', '222775', '222776', '222777', '222778', '222779', '222780']
    structure_freetext(list)
