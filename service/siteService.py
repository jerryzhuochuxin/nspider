from dao.redisDao import RedisDao


class SiteService:
    __site_url = 'https://api.bilibili.com/x/web-interface/online'
    __site_key = "site:start_urls"
    redis_connection = RedisDao.getRedisConnect()

    @classmethod
    def sendSiteInfoCrawlRequest(cls):
        cls.redis_connection.rpush(cls.__site_key, cls.__site_url)
