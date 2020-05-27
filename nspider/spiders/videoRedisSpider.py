import json
import logging
from datetime import datetime

from scrapy.cmdline import execute
from scrapy_redis.spiders import RedisSpider

from dao.mongoDbDao import MongoDbDao
from nspider.items import VideoItem
from service.taskService import SpiderTask
from service.videoService import VideoService
from util.constant import SubChannel2ChannelConstant


class VideoRedisSpider(RedisSpider):
    name = "videoRedis"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'nspider.pipelines.VideoPipeline': 300,
        }
    }

    def __init__(self):
        self.videoCollection = MongoDbDao.getCollection('video')
        self.task = SpiderTask('视频数据更新爬虫', collection=MongoDbDao.getCollection('tracer'))

    def parse(self, response):
        try:
            self.task.crawl_count += 1
            j = json.loads(response.body)
            d = j["data"]
            keys = list(d.keys())
            for each_key in keys:
                item = VideoItem()
                item['current_view'] = d[each_key]['stat']['view']
                item['current_favorite'] = d[each_key]['stat']['favorite']
                item['current_danmaku'] = d[each_key]['stat']['danmaku']
                item['current_coin'] = d[each_key]['stat']['coin']
                item['current_share'] = d[each_key]['stat']['share']
                item['current_like'] = d[each_key]['stat']['like']
                item['current_datetime'] = datetime.now()
                item['aid'] = d[each_key]['stat']['aid']
                item['mid'] = d[each_key]['owner']['mid']
                item['pic'] = d[each_key]['pic']
                item['author'] = d[each_key]['owner']['name']
                item['title'] = d[each_key]['title']
                item['subChannel'] = d[each_key]['tname']
                item['datetime'] = d[each_key]['pubdate']
                item['data'] = {
                    'view': item['current_view'],
                    'favorite': item['current_favorite'],
                    'danmaku': item['current_danmaku'],
                    'coin': item['current_coin'],
                    'share': item['current_share'],
                    'like': item['current_like'],
                    'datetime': item['current_datetime']
                }

                tid = d[each_key]['tid']
                if item['subChannel'] != '':
                    item['channel'] = SubChannel2ChannelConstant[item['subChannel']]
                elif item['subChannel'] == '资讯':
                    if tid == 51:
                        item['channel'] == '番剧'
                    elif tid == 170:
                        item['channel'] == '国创'
                    elif tid == 159:
                        item['channel'] == '娱乐'
                else:
                    item['channel'] = None

                url_list = response.url.split('&')
                if len(url_list) == 2:
                    item['object_id'] = url_list[1]
                else:
                    item['object_id'] = None
                yield item

        except Exception as error:
            self.task.crawl_failed += 1
            if j['code'] == -404:
                return
            logging.error("视频爬虫在解析时发生错误")
            logging.error(item)
            logging.error(response.url)
            logging.error(error)


if __name__ == "__main__":
    VideoService.updateVideo()
    VideoService.updateVideo(False)
    execute("scrapy crawl videoRedis".split())
