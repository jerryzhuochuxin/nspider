import random
from time import sleep
from venv import logger

import schedule

from dao.mongoDbDao import MongoDbDao
from service.authorService import AuthorService
from service.bangumiAndDonghuaService import BangumiAndDonghuaService
from service.fanWatcherService import FansWatcherService
from service.keywordAdderService import KeywordAdderService
from service.onlineService import OnlineService
from service.rankService import RankService
from service.siteService import SiteService
from service.tagAdderService import TagAdderService
from service.taskService import ExistsTask
from service.videoService import VideoService
from util.threadUtil import ThreadUtil


class ScheduleService:
    @classmethod
    def invoke(cls):
        schedule.every().day.at('01:00').do(ThreadUtil.newThreadToInvoke, AuthorService.updateAuthor)
        schedule.every().day.at('14:00').do(ThreadUtil.newThreadToInvoke, AuthorService.updateAutoAddAuthor)

        schedule.every().day.at('07:00').do(ThreadUtil.newThreadToInvoke, VideoService.updateVideo)
        schedule.every().day.at('22:00').do(ThreadUtil.newThreadToInvoke, VideoService.updateAutoAddVideo)
        schedule.every().week.do(ThreadUtil.newThreadToInvoke, VideoService.updateVideo, args=[False])

        schedule.every().wednesday.at('03:20').do(ThreadUtil.newThreadToInvoke, RankService.computeVideoRankTable)
        schedule.every().monday.at('03:20').do(ThreadUtil.newThreadToInvoke, RankService.calculateAuthorRank)

        schedule.every(15).minutes.do(ThreadUtil.newThreadToInvoke, OnlineService.genOnline)
        schedule.every(1).minutes.do(ThreadUtil.newThreadToInvoke, OnlineService.crawlOnlineTopListData)

        schedule.every().hour.do(ThreadUtil.newThreadToInvoke, SiteService.sendSiteInfoCrawlRequest)

        schedule.every().day.at('16:50').do(ThreadUtil.newThreadToInvoke, BangumiAndDonghuaService.autoCrawlBangumi)

        schedule.every().day.at('04:00').do(ThreadUtil.newThreadToInvoke, TagAdderService.addTagTask)

        schedule.every().day.at('12:00').do(ThreadUtil.newThreadToInvoke, FansWatcherService.watchBigAuthor)

        schedule.every().thursday.at('15:20').do(ThreadUtil.newThreadToInvoke, KeywordAdderService.addOmitted)

        ThreadUtil.newThreadToInvoke(cls.__autoCrawlTask)

    @classmethod
    def invokeDebug(cls, serviceList=[], startTime=1, endTime=3, useSecond=False):
        for serviceMap in serviceList:
            for serviceFunc in serviceMap:
                count = random.randint(startTime, endTime)
                if useSecond:
                    schedule.every(count).seconds.do(ThreadUtil.newThreadToInvoke, serviceFunc,
                                                     args=serviceMap[serviceFunc])
                else:
                    schedule.every(count).minutes.do(ThreadUtil.newThreadToInvoke, serviceFunc,
                                                     args=serviceMap[serviceFunc])

        ThreadUtil.newThreadToInvoke(cls.__autoCrawlTask)

    @classmethod
    def __autoCrawlTask(cls):
        task_name = "自动爬虫计划调度服务"
        logger.info(task_name)
        ExistsTask(task_name, update_frequency=60, collection=MongoDbDao.getCollection('tracer'))
        while True:
            schedule.run_pending()
            sleep(60)
