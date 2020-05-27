from scrapy.cmdline import execute
from scrapy_redis.spiders import RedisSpider

from nspider.items import TagAdderItem
from service.tagAdderService import TagAdderService


class TagAdderSpider(RedisSpider):
    name = "tagAdder"
    allowed_domains = ["bilibili.com"]

    custom_settings = {
        'ITEM_PIPELINES': {
            'nspider.pipelines.TagAdderPipeline': 300
        },
    }

    def parse(self, response):
        try:
            aid = str(response.url.lstrip('https://www.bilibili.com/video/av').rstrip('/')).split('?')[0]
            tagName = response.xpath("//li[@class='tag']/a/text()").extract()
            item = TagAdderItem()
            item['aid'] = int(aid)
            item['tag_list'] = []
            if tagName != []:
                ITEM_NUMBER = len(tagName)
                for i in range(0, ITEM_NUMBER):
                    item['tag_list'].append(tagName[i])
            yield item
        except Exception as error:
            print(error)
            item = TagAdderItem()
            item['aid'] = int(aid)
            item['tag_list'] = []
            yield item


if __name__ == "__main__":
    TagAdderService.addTagTask()
    execute("scrapy crawl tagAdder".split())
