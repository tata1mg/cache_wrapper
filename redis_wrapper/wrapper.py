import aioredis


class RedisWrapper:
    def __init__(self, host, port, conn=None):
        self._host = host
        self._port = port
        self._redis_connection = conn

    async def get_redis_connection(self):
        if not self._redis_connection:
            self._redis_connection = await aioredis.from_url(
                "redis://{}:{}".format(self._host, self._port), decode_responses=True
            )
        return await self._redis_connection

    async def sadd(self, key, value, namespace=None):
        if namespace is not None:
            key = self._get_key(namespace, key)
        redis = await self.get_redis_connection()
        await redis.sadd(key, value)

    async def set(self, key, value, ex=None, namespace=None, nx=False):
        if namespace is not None:
            key = self._get_key(namespace, key)
        redis = await self.get_redis_connection()
        return await redis.set(key, value, ex=ex, nx=nx)

    async def get(self, key, namespace=None):
        if namespace is not None:
            key = self._get_key(namespace, key)
        redis = await self.get_redis_connection()
        return await redis.get(key)

    async def incr(self, key, amount=1):
        # Set a redis key and increment the value by one
        redis = await self.get_redis_connection()
        value = await redis.incr(key, amount)
        return value

    async def increment_by_value(self, key, value: int):
        redis = await self.get_redis_connection()
        await redis.incrby(key, value)

    async def decr(self, key, amount=1):
        # Set a redis key and increment the value by one
        redis = await self.get_redis_connection()
        value = await redis.decr(key, amount)
        return value

    async def decrement_by_value(self, key, value: int):
        redis = await self.get_redis_connection()
        await redis.decrby(key, value)

    async def setnx(self, key, value):
        redis = await self.get_redis_connection()
        return await redis.setnx(key, value)

    async def delete(self, keys):
        redis = await self.get_redis_connection()
        await redis.delete(*keys)

    async def mset(self, mapping: dict):
        redis = await self.get_redis_connection()
        await redis.mset(mapping)

    async def mget(self, keys):
        redis = await self.get_redis_connection()
        return await redis.mget(keys)

    async def hset(self, key, mapping):
        redis = await self.get_redis_connection()
        await redis.hset(key, mapping=mapping)

    async def hget(self, key, field):
        redis = await self.get_redis_connection()
        return await redis.hget(key, field)

    async def hdel(self, key, fields):
        redis = await self.get_redis_connection()
        if key is not None:
            await redis.hdel(key, *fields)

    async def hgetall(self, key):
        redis = await self.get_redis_connection()
        return await redis.hgetall(key)

    async def hincrby(self, key, field, value: int = 1):
        redis = await self.get_redis_connection()
        return await redis.hincrby(key, field, value)

    async def hkeys(self, key):
        redis = await self.get_redis_connection()
        return await redis.hkeys(key)

    async def lpush(self, key, values):
        redis = await self.get_redis_connection()
        return await redis.lpush(key, *values)

    async def rpush(self, key, values):
        redis = await self.get_redis_connection()
        return await redis.rpush(key, *values)

    async def lpop(self, key):
        redis = await self.get_redis_connection()
        return await redis.lpop(key)

    async def brpop(self, keys):
        redis = await self.get_redis_connection()
        return await redis.brpop(keys)

    async def lrange(self, key, start, stop):
        redis = await self.get_redis_connection()
        return await redis.lrange(key, start, stop)

    async def clear_namespace(self, namespace) -> int:
        pattern = namespace + "*"
        return await self._delete_by_pattern(pattern)

    async def delete_by_prefix(self, prefix):
        pattern = "{}*".format(prefix)
        return await self._delete_by_pattern(pattern)

    async def _delete_by_pattern(self, pattern: str) -> int:
        if not pattern:
            return 0
        redis = await self.get_redis_connection()
        _keys = await redis.keys(pattern)
        if _keys:
            await redis.delete(*_keys)
        return len(_keys)

    async def exit(self):
        if self._redis_connection:
            await self._redis_connection.clear()

    async def keys(self, pattern: str):
        """
        Function to get all keys in redis matching to pattern_str
        :param pattern: keys pattern
        :return: list of redis keys
        """
        if pattern:
            redis = await self.get_redis_connection()
            return await redis.keys(pattern + "*")
        return []

    async def smembers(self, key, namespace=None):
        if namespace is not None:
            key = self._get_key(namespace, key)
        redis = await self.get_redis_connection()
        return await redis.smembers(key)

    async def sismember(self, key, value, namespace=None):
        if namespace is not None:
            key = self._get_key(namespace, key)
        redis = await self.get_redis_connection()
        return await redis.sismember(key, value)

    async def mset_with_expire(self, mapping: dict, ex=None):
        redis = await self.get_redis_connection()
        pipeline = redis.pipeline()
        for key, value in mapping.items():
            pipeline.set(key, value, ex=ex)
        await pipeline.execute()

    @staticmethod
    def _get_key(namespace, key):
        return namespace + ":" + key

    async def eval(self, script, numkeys, *keys_and_args):
        redis = await self.get_redis_connection()
        return await redis.eval(script, numkeys, *keys_and_args)

    async def expire(self, key, timeout):
        redis = await self.get_redis_connection()
        await redis.expire(key, timeout)

    async def zadd(self, key, value):
        redis = await self.get_redis_connection()
        return await redis.zadd(key, value)

    async def zpopmin(self, key, count=None):
        redis = await self.get_redis_connection()
        return await redis.zpopmin(key, count)

    async def zrange(self, key, limit, offset, withscores=False):
        redis = await self.get_redis_connection()
        return await redis.zrange(key, limit, offset, withscores=withscores)

    async def zpopmax(self, key, count=None):
        redis = await self.get_redis_connection()
        return await redis.zpopmax(key, count)

    async def spop(self, key, count=None):
        redis = await self.get_redis_connection()
        return await redis.spop(key, count)

    async def exists(self, *keys):
        redis = await self.get_redis_connection()
        return await redis.exists(*keys)
