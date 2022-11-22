import mysql.connector

userdb = 'root'
pwddb = 'Hello123'
hostdb = "127.0.0.1"
databasedb = 'openmrs'


def query(sql_query):
    cnx = mysql.connector.connect(user=userdb, password=pwddb, host=hostdb, database=databasedb)
    cursor = cnx.cursor()
    # print(sql_query)
    cursor.execute(sql_query)
    res = cursor.fetchall()
    return res