import random

import schedule

from service.authorService import AuthorService
from service.bangumiAndDonghuaService import BangumiAndDonghuaService
from service.fanWatcherService import FansWatcherService
from service.keywordAdderService import KeywordAdderService
from service.onlineService import OnlineService
from service.rankService import RankService
from service.siteService import SiteService
from service.tagAdderService import TagAdderService
from service.videoService import VideoService


class ServiceDebugUtil:
    __allServices = [
        {AuthorService.updateAutoAddAuthor: []},
        {AuthorService.updateAuthor: []},
        {VideoService.updateVideo: []},
        {VideoService.updateAutoAddVideo: []},
        {VideoService.updateAutoAddVideo: [False]},
        {RankService.computeVideoRankTable: []},
        {RankService.calculateAuthorRank: []},
        {OnlineService.genOnline: []},
        {OnlineService.crawlOnlineTopListData: []},
        {SiteService.sendSiteInfoCrawlRequest: []},
        {BangumiAndDonghuaService.autoCrawlBangumi: []},
        {TagAdderService.addTagTask: []},
        {FansWatcherService.watchBigAuthor: []},
        {KeywordAdderService.addOmitted: []}
    ]

    @classmethod
    def getAllService(cls):
        return cls.__allServices
