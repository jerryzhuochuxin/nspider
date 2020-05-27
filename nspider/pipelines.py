# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import datetime

from bson import ObjectId

from dao.mongoDbDao import MongoDbDao
from dao.redisDao import RedisDao


def sentCallBack(objectId, collection):
    if objectId != None and objectId != 'null':
        collection.update_one({'_id': ObjectId(objectId)}, {
            '$set': {'isExecuted': True}})


class AuthorPipeline(object):
    def __init__(self):
        self.authorCollection = MongoDbDao.getCollection('author')

    def process_item(self, item, spider):
        try:
            if (item['c_fans'] != 0):
                self.authorCollection.update_one({
                    'mid': item['mid']
                }, {
                    '$set': {
                        'focus': True,
                        'sex': item['sex'],
                        'name': item['name'],
                        'face': item['face'],
                        'level': item['level'],
                        'cFans': item['c_fans'],
                        'cLike': item['c_like'],
                        'cRate': item['c_rate'],
                        'official': item['official'],
                    },
                    '$push': {
                        'data': {
                            '$each': [item['data']],
                            '$position': 0
                        }
                    }
                }, True)
                if 'object_id' in item:
                    sentCallBack(item['object_id'], MongoDbDao.getCollection('user_record'))
                return item
        except Exception as error:
            logging.error('{}: {}'.format(spider.name, error))


class AuthorChannelPipeline(object):
    def __init__(self):
        self.authorCollection = MongoDbDao.getCollection('author')
        self.redisConnection = RedisDao.getRedisConnect()

    def process_item(self, item, spider):
        try:
            self.authorCollection.update_one({
                'mid': item['mid']
            }, {
                '$set': {
                    'channels': item['channels']
                },
            }, True)
            self.redisConnection.delete("author_detail::{}".format(item['mid']))
            return item
        except Exception as error:
            logging.error('{}: {}'.format(spider.name, error))


class VideoPipeline(object):
    def __init__(self):
        self.videoCollection = MongoDbDao.getCollection('video')
        self.redisConnection = RedisDao.getRedisConnect()

    def process_item(self, item, spider):
        try:
            self.videoCollection.update_one({
                'aid': int(item['aid'])
            }, {
                '$set': {
                    'cView': item['current_view'],
                    'cFavorite': item['current_favorite'],
                    'cDanmaku': item['current_danmaku'],
                    'cCoin': item['current_coin'],
                    'cShare': item['current_share'],
                    'cLike': item['current_like'],
                    'cDatetime': item['current_datetime'],
                    'author': item['author'],
                    'subChannel': item['subChannel'],
                    'channel': item['channel'],
                    'mid': item['mid'],
                    'pic': item['pic'],
                    'title': item['title'],
                    'datetime': datetime.datetime.fromtimestamp(
                        item['datetime'])
                },
                '$push': {
                    'data': {
                        '$each': [item['data']],
                        '$position': 0
                    }
                }
            }, True)
            if 'object_id' in item:
                sentCallBack(item['object_id'], MongoDbDao.getCollection('user_record'))
            return item
        except Exception as error:
            logging.error('{}: {}'.format(spider.name, error))


class VideoAddPipeline(object):
    def __init__(self):
        self.videoCollection = MongoDbDao.getCollection('video')
        self.redisConnection = RedisDao.getRedisConnect()

    def process_item(self, item, spider):
        try:
            for each_aid in item['aid']:
                self.videoCollection.update_one({
                    'aid': each_aid
                }, {
                    '$set': {
                        'aid': each_aid,
                        'focus': True
                    },
                }, True)
                self.redisConnection.lpush('videoRedis:start_urls',
                                           'https://api.bilibili.com/x/article/archives?ids={aid}'.format(
                                               aid=each_aid))
            return item

        except Exception as error:
            logging.error('{}: {}'.format(spider.name, error))


class OnlinePipeline(object):
    def __init__(self):
        self.videoOnlineCollection = MongoDbDao.getCollection('video_online')

    def process_item(self, item, spider):
        try:
            self.videoOnlineCollection.update_one({
                'title': item['title']
            }, {
                '$set': {
                    'title': item['title'],
                    'author': item['author'],
                    'channel': item['channel'],
                    'subChannel': item['subChannel'],
                },
                '$addToSet': {
                    'data': item['data']
                }
            }, True)
            return item
        except Exception as error:
            logging.error('{}: {}'.format(spider.name, error))


class SitePipeline(object):
    def __init__(self):
        self.coll = MongoDbDao.getCollection('site_info')

    def process_item(self, item, spider):
        try:
            self.coll.insert_one({
                'region_count': item['region_count'],
                'all_count': item['all_count'],
                'web_online': item['web_online'],
                'play_online': item['play_online'],
                'datetime': datetime.datetime.now()
            })
            return item
        except Exception as error:
            logging.error('{}: {}'.format(spider.name, error))


class BangumiAndDonghuaPipeLine(object):
    def __init__(self):
        self.mongoDbConnection = MongoDbDao.getMongoDb()

    def process_item(self, item, spider):
        try:
            self.mongoDbConnection[item['collection']].update_one({
                'title': item['title']
            }, {
                '$set': {
                    'title': item['title'],
                    'cover': item['cover'],
                    'newest': item['newest_ep_index'],
                    'currentPts': item['data']['pts'],
                    'currentPlay': item['data']['play'],
                    'currentWatch': item['data']['watch'],
                    'currentReview': item['data']['review'],
                    'currentDanmaku': item['data']['danmaku']
                },
                '$addToSet': {
                    'data': item['data']
                }
            }, True)
            return item
        except Exception as error:
            logging.error('{}: {}'.format(spider.name, error))


class TagAdderPipeline(object):
    def __init__(self):
        self.videoCollection = MongoDbDao.getCollection('video')

    def process_item(self, item, spider):
        try:
            self.videoCollection.update_one({
                'aid': item['aid']
            }, {
                '$set': {
                    'tag': item['tag_list'],
                },
            }, True)
            return item
        except Exception as error:
            logging.error('{}: {}'.format(spider.name, error))


class DanmakuAggregatePipeline(object):
    def __init__(self):
        self.coll = MongoDbDao.getCollection('video')

    def process_item(self, item, spider):
        self.coll.update_one({
            'aid': int(item['aid'])
        }, {
            '$set': {
                'danmaku_aggregate.{}'.format(item['page_number']): {
                    'duration': item['duration'],
                    'p_name': item['p_name'],
                    'danmaku_density': item['danmaku_density'],
                    'word_frequency': item['word_frequency']
                },
                'danmaku_aggregate.updatetime': datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
            }
        }, True)
        sentCallBack(item['object_id'], MongoDbDao.getCollection('user_record'))
