import datetime
import json
import logging

from scrapy.cmdline import execute
from scrapy_redis.spiders import RedisSpider

from dao.mongoDbDao import MongoDbDao
from nspider.items import BangumiOrDonghuaItem
from service.bangumiAndDonghuaService import BangumiAndDonghuaService
from service.taskService import SpiderTask


class BangumiAndDonghuaSpider(RedisSpider):
    name = "bangumiAndDonghua"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'nspider.pipelines.BangumiAndDonghuaPipeLine': 200
        }
    }

    def __init__(self):
        self.task = SpiderTask("番剧动画爬虫", collection=MongoDbDao.getCollection('tracer'))

    def parse(self, response):
        try:
            self.task.crawl_count += 1
            j = json.loads(
                response.xpath('//script/text()').extract()[2][len('window.__INITIAL_STATE__='):].split(";")[0])
            for each in j['rankList']:
                item = BangumiOrDonghuaItem()
                item['title'] = each['title']
                item['cover'] = each['cover']
                item['newest_ep_index'] = each['new_ep']['index_show']
                item['data'] = {
                    'danmaku': each['stat']['danmaku'],
                    'watch': each['stat']['follow'],
                    'play': each['stat']['view'],
                    'pts': each['pts'],
                    'review': each['video_review'],
                    'datetime': datetime.datetime.now()
                }

                if 'https://www.bilibili.com/ranking/bangumi/13/0/7' in response.url:
                    item['collection'] = 'bangumi'
                elif 'https://www.bilibili.com/ranking/bangumi/167/0/7' in response.url:
                    item['collection'] = 'donghua'
                yield item
        except Exception as error:
            logging.error(error)
            self.task.crawl_failed += 1


if __name__ == "__main__":
    BangumiAndDonghuaService.autoCrawlBangumi()
    execute("scrapy crawl bangumiAndDonghua".split())
