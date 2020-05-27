from venv import logger

from dao.mongoDbDao import MongoDbDao
from dao.redisDao import RedisDao
from service.taskService import ProgressTask


class TagAdderService:
    __videoCollection = MongoDbDao.getCollection('video')
    __tracerCollection = MongoDbDao.getCollection('tracer')
    __redisConnection = RedisDao.getRedisConnect()

    @classmethod
    def addTagTask(cls):
        task_name = "生成待爬标签视频链接"
        doc_filter = {'tag': {'$exists': False}}
        total = cls.__videoCollection.find(doc_filter, {"aid": 1}).count()
        cursor = cls.__videoCollection.find(doc_filter, {"aid": 1}).batch_size(100)

        progressTask = ProgressTask(task_name, total, collection=cls.__tracerCollection)
        url = 'https://www.bilibili.com/video/av{}'
        for each_video in cursor:
            progressTask.current_value += 1
            aid = each_video['aid']
            logger.info("待爬AV号{}".format(aid))
            cls.__redisConnection.rpush("tagAdder:start_urls", url.format(aid))
