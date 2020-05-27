import os

import redis


class RedisDao:
    __redisConnectString = os.environ.get("BILIOB_REDIS_CONNECTION_STRING")

    @classmethod
    def getRedisConnectString(cls):
        return cls.__redisConnectString

    @classmethod
    def getRedisConnect(cls):
        return redis.from_url(cls.__redisConnectString)
