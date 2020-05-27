from venv import logger

from dao.mongoDbDao import MongoDbDao
from dao.redisDao import RedisDao
from service.taskService import ProgressTask


class AuthorService:
    __author_url = "https://api.bilibili.com/x/web-interface/card?mid={mid}"
    __author_key = "authorRedis:start_urls"
    __redisConnection = RedisDao.getRedisConnect()

    @classmethod
    def updateAuthor(cls):
        task_name = "生成每日作者待爬链接"
        logger.info(task_name)
        coll = MongoDbDao.getCollection('author')
        filter_dict = {
            '$or': [{
                'focus': True
            }, {
                'forceFocus': True
            }]
        }
        cursor = coll.find(filter_dict, {"mid": 1}).batch_size(200)
        total = coll.count_documents(filter_dict)
        if total != 0:
            t = ProgressTask(task_name, total, collection=MongoDbDao.getCollection('tracer'))
            for each_doc in cursor:
                t.current_value += 1
                cls.pushAuthorRedisUrlToRedis(each_doc['mid'])

    @classmethod
    def pushAuthorRedisUrlToRedis(cls, mid):
        cls.__redisConnection.rpush(cls.__author_key, cls.__author_url.format(mid=mid))

    @classmethod
    def updateAutoAddAuthor(cls):
        start_urls = [
            'https://www.bilibili.com/ranking'
            'https://www.bilibili.com/ranking/all/1/0/3',
            'https://www.bilibili.com/ranking/all/168/0/3',
            'https://www.bilibili.com/ranking/all/3/0/3',
            'https://www.bilibili.com/ranking/all/129/0/3',
            'https://www.bilibili.com/ranking/all/188/0/3',
            'https://www.bilibili.com/ranking/all/4/0/3',
            'https://www.bilibili.com/ranking/all/36/0/3',
            'https://www.bilibili.com/ranking/all/160/0/3',
            'https://www.bilibili.com/ranking/all/119/0/3',
            'https://www.bilibili.com/ranking/all/155/0/3',
            'https://www.bilibili.com/ranking/all/5/0/3',
            'https://www.bilibili.com/ranking/all/181/0/3'
        ]
        for url in start_urls:
            RedisDao.getRedisConnect().rpush("authorAutoAdd:start_urls", url)
