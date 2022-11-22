import config as config
import mysql.connector



userdb = config.userdb
pwddb = config.pwddb
hostdb = config.hostdb
databasedb = config.databasedb


def query(sql_query):
    cnx = mysql.connector.connect(user=userdb, password=pwddb, host=hostdb, database=databasedb)
    cursor = cnx.cursor()
    # print(sql_query)
    cursor.execute(sql_query)
    res = cursor.fetchall()
    return res