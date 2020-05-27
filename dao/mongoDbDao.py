import os

from pymongo import MongoClient


class MongoDbDao:
    __mongodb = MongoClient(os.environ.get("BILIOB_MONGO_URL"))['biliob']

    @classmethod
    def getMongoDb(cls):
        return cls.__mongodb

    @classmethod
    def getCollection(cls, collName):
        return cls.__mongodb[collName]
