# coding=utf-8
import datetime
import json
import logging

from scrapy.cmdline import execute
from scrapy.http import Request
from scrapy_redis.spiders import RedisSpider

from dao.mongoDbDao import MongoDbDao
from nspider.items import AuthorItem
from service.authorService import AuthorService
from service.taskService import SpiderTask


class AuthorAutoAddSpider(RedisSpider):
    name = "authorAutoAdd"
    allowed_domains = ["bilibili.com"]

    custom_settings = {
        'ITEM_PIPELINES': {
            'nspider.pipelines.AuthorPipeline': 300
        },
        'DOWNLOAD_DELAY': 5
    }

    def __init__(self):
        RedisSpider.__init__(self)
        self.task = SpiderTask('活跃作者自动追加爬虫', collection=MongoDbDao.getCollection('tracer'))

    def parse(self, response):
        try:
            self.task.crawl_count += 1
            url_list = response.xpath(
                "//*[@id='app']/div[1]/div/div[1]/div[2]/div[3]/ul/li/div[2]/div[2]/div/a/@href"
            ).extract()

            # 为了爬取分区、粉丝数等数据，需要进入每一个视频的详情页面进行抓取
            for each_url in url_list:
                yield Request(
                    "https://api.bilibili.com/x/web-interface/card?mid=" + each_url[21:],
                    method='GET',
                    callback=self.detailParse)
        except Exception as error:
            self.task.crawl_failed += 1
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)

    def detailParse(self, response):
        try:
            j = json.loads(response.body)
            fans = j['data']['card']['fans']

            # 粉丝数大于1000才加入
            if int(fans) > 1000:
                item = AuthorItem()
                item['c_fans'] = int(fans)
                item['c_attention'] = int(j['data']['card']['attention'])
                item['c_archive'] = int(j['data']['archive_count'])
                item['c_article'] = int(j['data']['article_count'])
                item['mid'] = int(j['data']['card']['mid'])
                item['name'] = j['data']['card']['name']
                item['face'] = j['data']['card']['face']
                item['official'] = j['data']['card']['Official']['title']
                item['sex'] = j['data']['card']['sex']
                item['focus'] = True
                item['level'] = int(j['data']['card']['level_info']['current_level'])

                item['data'] = {
                    'fans': int(fans),
                    'attention': int(item['c_attention']),
                    'archive': int(item['c_archive']),
                    'article': int(item['c_article']),
                    'datetime': datetime.datetime.now()
                }

                # 目前还没有好的方法，先做一个兼容性插入，后期删除
                item['c_like'] = ""
                item['c_rate'] = 0

                yield item
        except Exception as error:
            self.task.crawl_failed += 1
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)


if __name__ == "__main__":
    AuthorService.updateAutoAddAuthor()
    execute("scrapy crawl authorAutoAdd".split())
