import json
import logging

from scrapy.cmdline import execute
from scrapy_redis.spiders import RedisSpider

from dao.mongoDbDao import MongoDbDao
from nspider.items import VideoWatcherItem
from service.taskService import SpiderTask
from service.videoService import VideoService


class VideoAutoAddSpider(RedisSpider):
    name = "videoAutoAdd"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'nspider.pipelines.VideoAddPipeline': 300,
            'nspider.pipelines.AuthorChannelPipeline': 301
        },
        'DOWNLOAD_DELAY': 0.5
    }

    def __init__(self):
        self.authorCollection = MongoDbDao.getCollection('author')
        self.task = SpiderTask('观测UP主的视频数据自动追加爬虫', collection=MongoDbDao.getCollection('tracer'))

    def parse(self, response):
        try:
            self.task.crawl_count += 1
            j = json.loads(response.body)

            item = VideoWatcherItem()
            if len(j['data']['vlist']) == 0:
                return

            channels = j['data']['tlist']
            item['channels'] = []
            for each_channel in channels:
                item['channels'].append(channels[each_channel])

            item['aid'] = []
            for each in j['data']['vlist']:
                item['aid'].append(int(each['aid']))
                item['mid'] = each['mid']

            yield item
        except Exception as error:
            self.task.crawl_failed += 1
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)


if __name__ == "__main__":
    VideoService.updateAutoAddVideo()
    execute("scrapy crawl videoAutoAdd".split())
