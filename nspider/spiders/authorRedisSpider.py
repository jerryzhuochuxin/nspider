import json
import logging
import datetime

from scrapy import Request
from scrapy.cmdline import execute
from scrapy_redis.spiders import RedisSpider

from dao.mongoDbDao import MongoDbDao
from dao.redisDao import RedisDao
from nspider.items import AuthorItem
from service.authorService import AuthorService
from service.taskService import SpiderTask


class AuthorRedisSpider(RedisSpider):
    name = "authorRedis"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'nspider.pipelines.AuthorPipeline': 300
        },
        'DOWNLOAD_DELAY': 0.1
    }

    def __init__(self):
        self.authorCollection = MongoDbDao.getCollection('author')
        self.redisCollection = RedisDao.getRedisConnect()
        self.task = SpiderTask('作者数据更新爬虫', collection=MongoDbDao.getCollection('tracer'))

    def parse(self, response):
        try:
            self.task.crawl_count += 1
            j = json.loads(response.body)
            item = AuthorItem()
            item['mid'] = int(j['data']['card']['mid'])
            item['name'] = j['data']['card']['name']
            item['face'] = j['data']['card']['face']
            item['official'] = j['data']['card']['Official']['title']
            item['sex'] = j['data']['card']['sex']
            item['level'] = int(j['data']['card']['level_info']['current_level'])
            item['c_fans'] = int(j['data']['card']['fans'])
            item['c_attention'] = int(j['data']['card']['attention'])
            item['c_archive'] = int(j['data']['archive_count'])
            item['c_article'] = int(j['data']['article_count'])
            item['data'] = {
                'fans': item['c_fans'],
                'attention': item['c_attention'],
                'archive': item['c_archive'],
                'article': item['c_article'],
                'datetime': datetime.datetime.now()
            }

            # 刷新redis数据缓存
            self.redisCollection.delete("author_detail::{}".format(item['mid']))

            url_list = response.url.split('&')
            if len(url_list) == 2:
                item['object_id'] = url_list[1]
            else:
                item['object_id'] = None

            if item['c_fans'] is not None:
                yield Request(
                    "https://api.bilibili.com/x/space/upstat?mid={mid}".format(mid=str(item['mid'])),
                    meta={'item': item},
                    method='GET',
                    callback=self.parse_view)
        except Exception as error:
            self.task.crawl_failed += 1
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)

    def parse_view(self, response):
        j = json.loads(response.body)
        item = response.meta['item']
        item['c_rate'] = 0
        item['c_like'] = j['data']['likes']
        item['data']['like'] = item['c_like']
        item['data']['archiveView'] = j['data']['archive']['view']
        item['c_archive_view'] = int(item['data']['archiveView'])
        item['data']['articleView'] = j['data']['article']['view']
        item['c_article_view'] = int(item['data']['articleView'])

        now = datetime.datetime.now()
        collectionResult = self.authorCollection.aggregate([
            {
                "$match": {
                    "mid": item['mid']
                }
            }, {
                "$unwind": "$data"
            }, {
                "$match": {
                    "data.datetime": {"$gt": now - datetime.timedelta(1.1)}
                }
            }, {
                "$sort": {"data.datetime": 1}
            }, {
                "$limit": 1
            }, {
                "$project": {"datetime": "$data.datetime", "like": "$data.like", "fans": "$data.fans",
                             "archiveView": "$data.archiveView", "articleView": "$data.articleView"}
            }
        ])

        for each in collectionResult:
            delta_seconds = now.timestamp() - each['datetime'].timestamp()
            delta_fans = item['data']['fans'] - each['fans']
            item['c_rate'] = int(delta_fans / delta_seconds * 86400)

        yield item


if __name__ == "__main__":
    AuthorService.updateAuthor()
    execute("scrapy crawl authorRedis".split())
