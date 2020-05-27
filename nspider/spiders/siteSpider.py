import json
import logging

from scrapy.cmdline import execute
from scrapy_redis.spiders import RedisSpider

from dao.mongoDbDao import MongoDbDao
from nspider.items import SiteItem
from service.siteService import SiteService
from service.taskService import SpiderTask


class SiteSpider(RedisSpider):
    name = "site"
    allowed_domains = ["bilibili.com"]
    custom_settings = {
        'ITEM_PIPELINES': {
            'nspider.pipelines.SitePipeline': 300
        }
    }

    def __init__(self):
        self.task = SpiderTask('全站信息爬虫', collection=MongoDbDao.getCollection('tracer'))

    def parse(self, response):
        try:
            self.task.crawl_count += 1
            d = json.loads(response.body)["data"]
            item = SiteItem()
            item['region_count'] = d['region_count']
            item['all_count'] = d['all_count']
            item['web_online'] = d['web_online']
            item['play_online'] = d['play_online']
            yield item

        except Exception as error:
            self.task.crawl_failed += 1
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)


if __name__ == "__main__":
    SiteService.sendSiteInfoCrawlRequest()
    execute("scrapy crawl site".split())
