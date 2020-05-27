# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class SiteItem(scrapy.Item):
    region_count = scrapy.Field()
    all_count = scrapy.Field()
    web_online = scrapy.Field()
    play_online = scrapy.Field()


class AuthorItem(scrapy.Item):
    object_id = scrapy.Field()
    c_rate = scrapy.Field()
    mid = scrapy.Field()
    name = scrapy.Field()
    face = scrapy.Field()
    official = scrapy.Field()
    sex = scrapy.Field()
    data = scrapy.Field()
    level = scrapy.Field()
    focus = scrapy.Field()
    pts = scrapy.Field()
    c_fans = scrapy.Field()
    c_attention = scrapy.Field()
    c_archive = scrapy.Field()
    c_article = scrapy.Field()
    c_archive_view = scrapy.Field()
    c_article_view = scrapy.Field()
    c_like = scrapy.Field()
    c_datetime = scrapy.Field()


class VideoItem(scrapy.Item):
    object_id = scrapy.Field()
    channel = scrapy.Field()
    aid = scrapy.Field()
    datetime = scrapy.Field()
    author = scrapy.Field()
    data = scrapy.Field()
    subChannel = scrapy.Field()
    title = scrapy.Field()
    mid = scrapy.Field()
    pic = scrapy.Field()
    current_view = scrapy.Field()
    current_favorite = scrapy.Field()
    current_danmaku = scrapy.Field()
    current_coin = scrapy.Field()
    current_share = scrapy.Field()
    current_like = scrapy.Field()
    current_datetime = scrapy.Field()


class VideoWatcherItem(scrapy.Item):
    mid = scrapy.Field()
    aid = scrapy.Field()
    channels = scrapy.Field()


class OnlineItem(scrapy.Item):
    title = scrapy.Field()
    author = scrapy.Field()
    data = scrapy.Field()
    aid = scrapy.Field()
    subChannel = scrapy.Field()
    channel = scrapy.Field()


class BangumiOrDonghuaItem(scrapy.Item):
    title = scrapy.Field()
    tag = scrapy.Field()
    cover = scrapy.Field()
    square_cover = scrapy.Field()
    is_finish = scrapy.Field()
    is_started = scrapy.Field()
    newest_ep_index = scrapy.Field()
    data = scrapy.Field()
    collection = scrapy.Field()


class TagAdderItem(scrapy.Item):
    tag_list = scrapy.Field()
    aid = scrapy.Field()


class DanmakuAggregateItem(scrapy.Item):
    aid = scrapy.Field()
    p_name = scrapy.Field()
    page_number = scrapy.Field()
    word_frequency = scrapy.Field()
    danmaku_density = scrapy.Field()
    duration = scrapy.Field()
    object_id = scrapy.Field()
