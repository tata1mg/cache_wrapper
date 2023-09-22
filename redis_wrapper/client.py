import ujson as json

from redis_wrapper.utils import RedisLogger

from .cache_hosts import cache_hosts


class RedisCache:
    _host = "global"
    _service_prefix = "service"
    _key_prefix = "base"
    _delimiter = ":"
    _expire_in_sec = None
    _mset_with_expire_max_keys_limit = 100

    @classmethod
    def prefixed_key(cls, key: str):
        return (
            cls._service_prefix
            + cls._delimiter
            + cls._key_prefix
            + cls._delimiter
            + key
        )

    @classmethod
    @RedisLogger.log
    async def set(cls, key, value, expire=None, namespace=None, nx=False):
        """
        Sets a key value pair.
        :param key: String
        :param value: Any (Serializable to String using str())
        :param expire: If provided, key will expire in given number of seconds. _expire_in_sec can also be set at class
        level to avoid passing it to this function everytime. If none is provided, key will live forever.
        :param namespace:
        :param nx: if set to True, set the value at key ``name`` to ``value`` only if it does not exist.
        """
        if not expire:
            expire = cls._expire_in_sec
        await cache_hosts[cls._host].set(
            cls.prefixed_key(key),
            json.dumps(value),
            ex=expire,
            namespace=namespace,
            nx=nx,
        )

    @classmethod
    @RedisLogger.log
    async def set_with_result(cls, key, value, expire=None, namespace=None, nx=False):
        """
        Sets a key value pair.
        :param key: String
        :param value: Any (Serializable to String using str())
        :param expire: If provided, key will expire in given number of seconds. _expire_in_sec can also be set at class
        level to avoid passing it to this function everytime. If none is provided, key will live forever.
        :param namespace:
        :param nx: if set to True, set the value at key ``name`` to ``value`` only if it does not exist.
        """
        if not expire:
            expire = cls._expire_in_sec
        return await cache_hosts[cls._host].set(
            cls.prefixed_key(key),
            json.dumps(value),
            ex=expire,
            namespace=namespace,
            nx=nx,
        )

    @classmethod
    @RedisLogger.log
    async def get(cls, key):
        """
        Return the value at key, or None if the key doesn't exist
        :param key: String
        :return: Any (Serialized to original data type which was set)
        """
        result = await cache_hosts[cls._host].get(cls.prefixed_key(key))
        if result:
            result = json.loads(result)
        return result

    @classmethod
    @RedisLogger.log
    async def incr(cls, key, amount: int = 1):
        """
        Increments the value of key by amount.  If no key exists,
        the value will be initialized as amount
        :param key: String
        :param amount: Integer
        """
        return await cache_hosts[cls._host].incr(cls.prefixed_key(key), amount=amount)

    @classmethod
    @RedisLogger.log
    async def decr(cls, key, amount: int = 1):
        """
        Decrements the value of key by amount.  If no key exists,
        the value will be initialized as 0
        :param key: String
        :param amount: Integer
        """
        return await cache_hosts[cls._host].decr(cls.prefixed_key(key), amount=amount)

    @classmethod
    @RedisLogger.log
    async def setnx(cls, key, value):
        """
        Set a key value pair if key doesn't exist
        :param key: String
        :param value: Any (Serializable to String using str())
        """
        await cache_hosts[cls._host].setnx(cls.prefixed_key(key), json.dumps(value))

    @classmethod
    @RedisLogger.log
    async def delete(cls, keys: list):
        """
        Delete one or more keys specified by keys
        :param keys: list of str
        """
        keys = list(map(lambda key: cls.prefixed_key(key), keys))
        await cache_hosts[cls._host].delete(keys)

    @classmethod
    @RedisLogger.log
    async def keys(cls, pattern="*"):
        """
        Returns a list of keys matching pattern
        :param pattern: String
        :return: List of str
        """
        result = await cache_hosts[cls._host].keys(pattern=cls.prefixed_key(pattern))
        return result

    @classmethod
    @RedisLogger.log
    async def mset(cls, mapping: dict):
        """
        Sets key/values based on a mapping. Mapping is a dictionary of
        key/value pairs. Both keys and values should be strings or types that
        can be cast to a string via str().
        :param mapping: dict
        """
        mapping = {cls.prefixed_key(k): json.dumps(v) for k, v in mapping.items()}
        await cache_hosts[cls._host].mset(mapping)

    @classmethod
    @RedisLogger.log
    async def mget(cls, keys: list):
        """
        Returns a list of values ordered identically to keys
        :param keys: list of str
        :return: list of any
        """
        keys = list(map(lambda key: cls.prefixed_key(key), keys))
        result = await cache_hosts[cls._host].mget(keys)
        if result:
            result = list(
                map(lambda value: json.loads(value) if value else None, result)
            )
        return result

    @classmethod
    @RedisLogger.log
    async def hset(cls, key, mapping: dict):
        """
        Sets a key value pair within hash key,
        mapping accepts a dict of key/value pairs that that will be
        added to hash key.
        Returns the number of fields that were added.
        :param key: String
        :param mapping: dict {key: String, value: Any (Serializable to String using str())}
        """
        mapping = {k: json.dumps(v) for k, v in mapping.items()}
        await cache_hosts[cls._host].hset(cls.prefixed_key(key), mapping)

    @classmethod
    @RedisLogger.log
    async def hget(cls, key, field):
        """
        Return the value of filed within the hash key
        :param key: String
        :param field: String
        :return: Any (Serialized to original data type which was set)
        """
        result = await cache_hosts[cls._host].hget(cls.prefixed_key(key), field)
        if result:
            result = json.loads(result)
        return result

    @classmethod
    @RedisLogger.log
    async def hdel(cls, key, fields):
        """
        Delete one or more fields from hash key
        :param key: String
        :param fields: list of str
        """
        await cache_hosts[cls._host].hdel(cls.prefixed_key(key), fields)

    @classmethod
    @RedisLogger.log
    async def hgetall(cls, key):
        """
        Return a Python dict of the hash's field/value pairs
        :param key: String
        :return: dict {key: String, value: Any (Serialized to original data type which was set)}
        """
        result = await cache_hosts[cls._host].hgetall(cls.prefixed_key(key))
        if result:
            result = {k.decode("utf-8"): json.loads(v) for k, v in result.items()}
        return result

    @classmethod
    @RedisLogger.log
    async def hincrby(cls, key, field, value: int = 1):
        """
        Increment the value of field in hash key by amount
        :param key: String
        :param field: String
        :param value: Integer
        """
        await cache_hosts[cls._host].hincrby(cls.prefixed_key(key), field, value)

    @classmethod
    @RedisLogger.log
    async def hkeys(cls, key):
        """
        Return the list of keys within hash key
        :param key: String
        :return: list of str
        """
        return await cache_hosts[cls._host].hkeys(cls.prefixed_key(key))

    @classmethod
    @RedisLogger.log
    async def lpush(cls, key, values: list):
        """
        Push values onto the head of the list key
        :param key: String
        :param values: list of any
        """
        await cache_hosts[cls._host].lpush(cls.prefixed_key(key), values)

    @classmethod
    @RedisLogger.log
    async def rpush(cls, key, values: list):
        """
        Push values onto the tail of the list key
        :param key:  String
        :param values: list of any
        """
        await cache_hosts[cls._host].rpush(cls.prefixed_key(key), values)

    @classmethod
    @RedisLogger.log
    async def lpop(cls, key):
        """
        Remove and return the first item of the list key
        :param key: String
        :return: any
        """
        result = await cache_hosts[cls._host].lpop(cls.prefixed_key(key))
        return result

    @classmethod
    @RedisLogger.log
    async def lrange(cls, key, start: int = 0, end: int = -1):
        """
        Return a slice of the list key between
        position start and end

        start and end can be negative numbers just like
        Python slicing notation
        :param key: String
        :param start: int
        :param end: int
        :return: list of any
        """
        result = await cache_hosts[cls._host].lrange(cls.prefixed_key(key), start, end)
        return result

    @classmethod
    @RedisLogger.log
    async def delete_by_prefix(cls, prefix):
        """
        :param prefix:
        :return:
        """
        result = await cache_hosts[cls._host].delete_by_prefix(cls.prefixed_key(prefix))
        return result

    @classmethod
    @RedisLogger.log
    async def members_in_set(cls, key, namespace=None):
        """
        :param key:
        :param namespace:
        :return:
        """
        result = await cache_hosts[cls._host].smembers(
            cls.prefixed_key(key), namespace=namespace
        )
        return result

    @classmethod
    @RedisLogger.log
    async def is_value_in_set(cls, key, value, namespace=None):
        """
        :param key:
        :param value:
        :param namespace:
        :return:
        """
        result = await cache_hosts[cls._host].sismember(
            cls.prefixed_key(key), value, namespace=namespace
        )
        return result

    @classmethod
    @RedisLogger.log
    async def mset_with_expire(cls, mapping: dict, expire=None):
        if not expire:
            expire = cls._expire_in_sec

        if len(mapping.keys()) > cls._mset_with_expire_max_keys_limit:
            raise Exception(
                f"Please use batch processing for keys count > {cls._mset_with_expire_max_keys_limit}"
            )
        mapping = {cls.prefixed_key(k): json.dumps(v) for k, v in mapping.items()}
        await cache_hosts[cls._host].mset_with_expire(mapping, expire)

    @classmethod
    @RedisLogger.log
    async def eval(cls, script, numkeys, *keys_and_args):
        """
        :param script:
        :param numkeys:
        :param keys_and_args
        :return:
        """
        result = await cache_hosts[cls._host].eval(script, numkeys, *keys_and_args)

    @classmethod
    @RedisLogger.log
    async def zadd(cls, key, element):
        """
        add one or more member to sorted set & update score if key already exists
        :param element: element(s) with score
        :param key: String
        :return: any
        """
        result = await cache_hosts[cls._host].zadd(cls.prefixed_key(key), element)
        return result

    @classmethod
    @RedisLogger.log
    async def zpopmin(cls, key, count=None):
        """
        Remove and return the count number of members with minimum score
        :param key: String
        :count: integer
        :return: min score elements from sorted redis
        """
        result = await cache_hosts[cls._host].zpopmin(cls.prefixed_key(key), count)
        return result

    @classmethod
    @RedisLogger.log
    async def zrange(cls, key, limit, offset, withscores=False):
        """
        retrieve a range of members from a sorted set
        :param limit: starting index of the range to retrieve
        :param offset: ending index of the range to retrieve
        :param key: name of the sorted set
        :return: min score element from sorted redis
        """
        result = await cache_hosts[cls._host].zrange(
            cls.prefixed_key(key), limit, offset, withscores=withscores
        )
        return result

    @classmethod
    @RedisLogger.log
    async def zpopmax(cls, key, count=None):
        """
        Remove and return the count number of members with maximum score
        :param key: String
        :param count: integer
        :return: max score elements from sorted redis
        """
        result = await cache_hosts[cls._host].zpopmax(
            cls.prefixed_key(key), count=count
        )
        return result

    @classmethod
    @RedisLogger.log
    async def sadd(cls, key, *args):
        """
        add one or more member to set
        :param args: element(s)
        :param key: String
        :return: any
        """
        for value in args:
            await cache_hosts[cls._host].sadd(cls.prefixed_key(key), value)

    @classmethod
    @RedisLogger.log
    async def spop(cls, key, count=None):
        """
        Remove and return the count number of members with maximum score
        :param key: String
        :param count: integer
        :return: elements from set redis
        """
        result = await cache_hosts[cls._host].spop(cls.prefixed_key(key), count)
        return result

    @classmethod
    @RedisLogger.log
    async def expire(cls, key, expire):
        """
        Set expire time for a given key
        :param key: String
        :param expire: integer ( expiry time in seconds )
        :return: 1 = key found and expiry set for the key
                 0 = expiry time not set because key not found
        """
        result = await cache_hosts[cls._host].expire(cls.prefixed_key(key), expire)
        return result

    @classmethod
    @RedisLogger.log
    async def is_key_exist(cls, key):
        """
        check if a single key exists in redis cache
        :param key: String
        :return: 1 = key exists in cache
                 0 = key does not exist in cache
        """
        result = await cache_hosts[cls._host].exists(cls.prefixed_key(key))
        return result
