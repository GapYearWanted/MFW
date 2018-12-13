# __author__ = 'chendansi'


import pymongo
from MFW.utils.CONFIG import MONGO_HOST, MONGO_PORT


def connect_table(tablename):
    client = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    return client["crawler"][tablename]


if __name__ == "__main__":
    table = connect_table("mfw.mdd.jd")
    for i in table.distinct("name"):
        print(i)