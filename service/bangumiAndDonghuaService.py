from venv import logger

from dao.mongoDbDao import MongoDbDao
from dao.redisDao import RedisDao
from service.taskService import ProgressTask


class BangumiAndDonghuaService:
    __tracerCollection = MongoDbDao.getCollection('tracer')
    __redisConnection = RedisDao.getRedisConnect()
    __bangumiAndDonghua_key = "bangumiAndDonghua:start_urls"

    @classmethod
    def autoCrawlBangumi(cls):
        task_name = "生成番剧国创待爬链接"
        logger.info(task_name)
        progressTask = ProgressTask(task_name, 1, collection=cls.__tracerCollection)

        urls = ["https://www.bilibili.com/ranking/bangumi/167/0/7", "https://www.bilibili.com/ranking/bangumi/13/0/7"]
        for url in urls:
            cls.__redisConnection.rpush(cls.__bangumiAndDonghua_key, url)

        progressTask.current_value += 1
