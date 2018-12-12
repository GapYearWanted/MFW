# __author__ = 'chendansi'


import pymongo
from MFW.utils.CONFIG import MONGO_HOST, MONGO_PORT

HOST = "127.0.0.1"
PORT = 27017


def connect_table(tablename):
    client = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    return client["crawler"][tablename]