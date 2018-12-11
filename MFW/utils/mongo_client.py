# __author__ = 'chendansi'


import pymongo


HOST = "127.0.0.1"
PORT = 27017


def connect_table(tablename):
    client = pymongo.MongoClient(host=HOST, port=PORT)
    return client["crawler"][tablename]