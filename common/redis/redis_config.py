import redis
from redis import ConnectionPool
import os

class RedisClient:
    def __init__(self):
        self.host = os.getenv('REDIS_HOST', '127.0.0.1')
        self.port = os.getenv('REDIS_PORT', 6379)
        self.db = os.getenv('REDIS_DB', 0)

        self.pool = ConnectionPool(host=self.host, port=self.port, db=self.db)
        self.client = redis.Redis(connection_pool=self.pool)

    def pipeline(self):
        return self.client.pipeline()

redis_client = RedisClient().client
