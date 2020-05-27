import datetime
import logging

from scrapy import Request
from scrapy.cmdline import execute
from scrapy_redis.spiders import RedisSpider

from dao.mongoDbDao import MongoDbDao
from nspider.items import OnlineItem
from service.onlineService import OnlineService
from service.taskService import SpiderTask


class OnlineSpider(RedisSpider):
    name = "online"
    allowed_domains = ["bilibili.com"]
    custom_settings = {
        'ITEM_PIPELINES': {
            'nspider.pipelines.OnlinePipeline': 300
        }
    }

    def __init__(self):
        self.task = SpiderTask('同时在线人数爬虫', collection=MongoDbDao.getCollection('tracer'))

    def parse(self, response):
        try:
            self.task.crawl_count += 1
            title_list = response.xpath('//*[@id="app"]/div/div[2]/div/a/p/text()').extract()
            author_list = response.xpath('//*[@id="app"]/div/div[2]/div/div[1]/a/text()').extract()
            watch_list = response.xpath('//*[@id="app"]/div/div[2]/div/p/b/text()').extract()
            href_list = response.xpath('//*[@id="app"]/div/div[2]/div/a/@href').extract()

            for i in range(len(title_list)):
                item = OnlineItem()
                item['title'] = title_list[i]
                item['author'] = author_list[i]
                item['aid'] = href_list[i][21:-1]
                item['data'] = {
                    'datetime': datetime.datetime.now(),
                    'number': watch_list[i]
                }
                yield Request("https:" + href_list[i], meta={'item': item}, callback=self.detailParse)
        except Exception as error:
            self.task.crawl_failed += 1
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)

    def detailParse(self, response):
        try:
            item = response.meta['item']
            content = response.xpath('//*[@id="media_module"]/div/div[2]/a[1]/text()').extract()
            if content != []:
                item['channel'] = content
            else:
                item['channel'] = '番剧'

            content = response.xpath('//*[@id="media_module"]/div/div[3]/a[1]/text()').extract()
            if content != []:
                item['subChannel'] = content
            else:
                item['subChannel'] = '番剧'

            yield item
        except Exception as error:
            logging.error("视频爬虫在解析细节时发生错误")
            logging.error(response.url)
            logging.error(error)


if __name__ == "__main__":
    OnlineService.genOnline()
    execute("scrapy crawl online".split())
