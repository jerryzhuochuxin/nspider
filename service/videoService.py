from venv import logger

from dao.mongoDbDao import MongoDbDao
from dao.redisDao import RedisDao
from service.taskService import ProgressTask


class VideoService:
    __video_url = "https://api.bilibili.com/x/article/archives?ids={aid}"
    __video_key = "videoRedis:start_urls"
    __redisConnection = RedisDao.getRedisConnect()
    __tracerCollection = MongoDbDao.getCollection('tracer')

    @classmethod
    def updateVideo(cls, focus=True):
        if focus:
            task_name = "生成每日视频待爬链接"
        else:
            task_name = "生成保守观测视频待爬链接"
        logger.info(task_name)

        doc_filter = {'focus': focus}
        videoCollection = MongoDbDao.getCollection('video')
        total = videoCollection.count_documents(doc_filter)
        cursor = videoCollection.find(doc_filter, {"aid": 1}).batch_size(200)

        if total == 0:
            return

        countNum = 0
        aid_list = ''
        progressTask = ProgressTask(task_name, total, collection=cls.__tracerCollection)
        for each_doc in cursor:
            aid_list += str(each_doc['aid']) + ','
            countNum += 1
            logger.info(each_doc['aid'])
            if countNum == 50:
                progressTask.current_value += countNum
                cls.pushVideoRedisUrlToRedis(aid_list[:-1])
                aid_list = ''
                countNum = 0

        progressTask.current_value += countNum
        cls.pushVideoRedisUrlToRedis(aid_list[:-1])

    @classmethod
    def pushVideoRedisUrlToRedis(cls, aid):
        cls.__redisConnection.rpush(cls.__video_key, cls.__video_url.format(aid=aid))

    @classmethod
    def updateAutoAddVideo(cls):
        task_name = "生成作者最新发布的视频的待爬链接"
        logger.info(task_name)
        authorCollection = MongoDbDao.getCollection('author')
        doc_filter = {'$or': [{'focus': True}, {'forceFocus': True}]}
        total = authorCollection.count_documents(doc_filter)
        authorCollectionResult = authorCollection.find(doc_filter, {'mid': 1})
        if total != 0:
            progressTask = ProgressTask(task_name, total, collection=cls.__tracerCollection)
            for each_doc in authorCollectionResult:
                progressTask.current_value += 1
                url = 'https://space.bilibili.com/ajax/member/getSubmitVideos?mid={}&pagesize=10&page=1&order=pubdate'.format(
                    each_doc['mid'])
                cls.__redisConnection.rpush("videoAutoAdd:start_urls", url)
