import json
from venv import logger

import requests
from lxml import etree

from dao.mongoDbDao import MongoDbDao
from dao.redisDao import RedisDao
from service.authorService import AuthorService
from service.taskService import ProgressTask
from service.videoService import VideoService


class OnlineService:
    __online_url = 'https://www.bilibili.com/video/online.html'
    __online_key = "online:start_urls"
    __redisConnection = RedisDao.getRedisConnect()
    __tracerCollection = MongoDbDao.getCollection('tracer')

    @classmethod
    def genOnline(cls):
        task_name = "生成在线人数爬取链接"
        progressTask = ProgressTask(task_name, 1, collection=cls.__tracerCollection)
        cls.__redisConnection.rpush(cls.__online_key, cls.__online_url)
        progressTask.current_value = 1

    @classmethod
    def crawlOnlineTopListData(cls):
        task_name = "生成强力追踪待爬链接"
        logger.info(task_name)
        response = requests.get(cls.__online_url)
        data_text = etree.HTML(response.content.decode('utf8')).xpath('//script/text()')[-2]
        j = json.loads(data_text.lstrip('window.__INITIAL_STATE__=')[:-122])
        total = len(j['onlineList'])
        progressTask = ProgressTask(task_name, total, collection=cls.__tracerCollection)

        for each_video in j['onlineList']:
            mid = each_video['owner']['mid']
            if mid not in [7584632, 928123]:
                AuthorService.pushAuthorRedisUrlToRedis(mid)
            VideoService.pushVideoRedisUrlToRedis(each_video['aid'])
            progressTask.current_value += 1


if __name__ == "__main__":
    OnlineService.crawlOnlineTopListData()
