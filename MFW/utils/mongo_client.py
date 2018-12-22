# __author__ = 'chendansi'


import pymongo
from MFW.utils.CONFIG import MONGO_HOST, MONGO_PORT, MONGO_PASSWD, MONGO_USER


def connect_table(tablename):
    client = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    db = client.crawler
    db.authenticate(MONGO_USER, MONGO_PASSWD)
    return db[tablename]


if __name__ == "__main__":
    table = connect_table("mfw.mdd.jd")
    for i in table.distinct("name"):
        print(i)