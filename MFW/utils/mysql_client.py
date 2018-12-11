# __author__ = 'chendansi'

import pymysql
import dataset

pymysql.install_as_MySQLdb()


HOST = "127.0.0.1"
PORT = 3306
USER = "root"
PASSWD = "123456"


def connect(dbname):
    db = pymysql.connect(host=HOST,
                         port=PORT,
                         user=USER,
                         passwd=PASSWD,
                         db=dbname)
    cursor = db.cursor()
    return cursor

def get_table(dbname, tablename):
    db = dataset.connect(f'mysql://{USER}:{PASSWD}@{HOST}:{PORT}/{dbname}')
    table = db[tablename]
    return table