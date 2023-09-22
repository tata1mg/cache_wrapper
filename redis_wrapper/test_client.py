import asyncio

import aiounittest
import fakeredis.aioredis

from .cache_hosts import cache_hosts
from .client import RedisCache
from .wrapper import RedisWrapper


class TestClient(aiounittest.AsyncTestCase):
    def setUp(self):
        self.redis = RedisWrapper(
            "localhost", 6544, conn=fakeredis.aioredis.FakeRedis()
        )
        RedisCache._host = "global"
        cache_hosts["global"] = self.redis

    def tearDown(self):
        del self.redis

    async def test_set(self):
        await RedisCache.set("testKey", "testValue")
        self.assertEqual(await RedisCache.get("testKey"), "testValue")

    async def test_set_with_result(self):
        result = await RedisCache.set_with_result("testKey", "testValue")
        self.assertEqual(result, "testValue")

    async def test_set_list(self):
        await RedisCache.set("testKey", ["testValue"])
        self.assertEqual(await RedisCache.get("testKey"), ["testValue"])

    async def test_set_with_expire(self):
        await RedisCache.set("testKey1", "testValue1", expire=2)
        await asyncio.sleep(3)
        self.assertEqual(await RedisCache.get("testKey"), None)

    async def test_get_type_with_dict_value(self):
        payload = {"testKey1": "testValue1", "testKey2": {"testKey22": "testValue22"}}
        await RedisCache.set("test", payload)
        self.assertEqual(type(await RedisCache.get("test")), dict)

    async def test_get_data_with_dict_value(self):
        payload = {"testKey1": "testValue1", "testKey2": {"testKey22": "testValue22"}}
        await RedisCache.set("test", payload)
        result = await RedisCache.get("test")
        self.assertEqual(result.get("testKey2").get("testKey22"), "testValue22")

    async def test_increment_by(self):
        await RedisCache.set("testKey", 1)
        await RedisCache.incr("testKey")
        self.assertEqual(await RedisCache.get("testKey"), 2)

    async def test_increment_by_with_amount(self):
        await RedisCache.set("testKey", 1)
        await RedisCache.incr("testKey", 10)
        self.assertEqual(await RedisCache.get("testKey"), 11)

    async def test_decrement_by(self):
        await RedisCache.set("testKey", 2)
        await RedisCache.decr("testKey")
        self.assertEqual(await RedisCache.get("testKey"), 1)

    async def test_decrement_by_with_amount(self):
        await RedisCache.set("testKey", 10)
        await RedisCache.decr("testKey", 3)
        self.assertEqual(await RedisCache.get("testKey"), 7)

    async def test_set_if_not_exist(self):
        await RedisCache.setnx("testKey", 1)
        self.assertEqual(await RedisCache.get("testKey"), 1)

    async def test_set_if_not_exist_with_key_already_exist(self):
        await RedisCache.set("testKey", 1)
        await RedisCache.setnx("testKey", 20)
        self.assertEqual(await RedisCache.get("testKey"), 1)

    async def test_delete(self):
        payload = {
            "banner1": {"banner1": {"banner11": "value11"}},
            "banner2": {"banner2": "value2"},
            "test": "test1",
        }
        await RedisCache.mset(payload)
        await RedisCache.delete(["banner1", "banner2"])
        # FakeRedis return response of keys() in bytes while aioredis return string.
        self.assertEqual(await RedisCache.keys(), [b"service:base:test"])

    async def test_keys_with_pattern(self):
        payload = {
            "banner1": {"banner1": {"banner11": "value11"}},
            "banner2": {"banner2": "value2"},
            "test1": "test1",
        }
        await RedisCache.mset(payload)
        # FakeRedis return response of keys() in bytes while aioredis return string.
        self.assertEqual(await RedisCache.keys(pattern="test"), [b"service:base:test1"])

    async def test_mset(self):
        payload = {"testKey1": "testValue1", "testKey2": {"testKey22": "testValue22"}}
        await RedisCache.mset(payload)
        # FakeRedis return response of keys() in bytes while aioredis return string.
        self.assertEqual(
            await RedisCache.keys(),
            [b"service:base:testKey1", b"service:base:testKey2"],
        )

    async def test_mget(self):
        payload = {"testKey1": "testValue1", "testKey2": "testValue2"}
        await RedisCache.mset(payload)
        self.assertEqual(
            await RedisCache.mget(["testKey1", "testKey2", "testKey3"]),
            ["testValue1", "testValue2", None],
        )

    async def test_hset_type(self):
        payload = {"testKey1": "testValue1", "testKey2": "testValue2"}
        await RedisCache.hset("testhash", payload)
        result = await RedisCache.hgetall("testhash")
        self.assertEqual(type(result), dict)

    async def test_hgetall_value(self):
        payload = {"testKey1": "testValue1", "testKey2": "testValue2"}
        await RedisCache.hset("testhash", payload)
        result = await RedisCache.hgetall("testhash")
        self.assertEqual(result["testKey1"], "testValue1")

    async def test_hget(self):
        payload = {"testKey1": "testValue1", "testKey2": "testValue2"}
        await RedisCache.hset("testhash", payload)
        self.assertEqual(await RedisCache.hget("testhash", "testKey2"), "testValue2")

    async def test_hget_dict(self):
        payload = {"testKey1": "testValue1", "testKey2": {"testKey22": "testValue22"}}
        await RedisCache.hset("testhash", payload)
        self.assertEqual(
            await RedisCache.hget("testhash", "testKey2"), {"testKey22": "testValue22"}
        )

    async def test_hincrby(self):
        payload = {"testKey1": 1, "testKey2": 2}
        await RedisCache.hset("testhash", payload)
        await RedisCache.hincrby("testhash", "testKey2")
        self.assertEqual(await RedisCache.hget("testhash", "testKey2"), 3)

    async def test_hincrby_with_value(self):
        payload = {"testKey1": 1, "testKey2": 2}
        await RedisCache.hset("testhash", payload)
        await RedisCache.hincrby("testhash", "testKey2", 2)
        self.assertEqual(await RedisCache.hget("testhash", "testKey2"), 4)

    async def test_hdel(self):
        payload = {"testKey1": 1, "testKey2": 2}
        await RedisCache.hset("testhash", payload)
        await RedisCache.hdel("testhash", ["testKey1"])
        self.assertEqual(await RedisCache.hget("testhash", "testKey1"), None)

    async def test_hdel_multiple(self):
        payload = {"testKey1": 1, "testKey2": 2, "testKey3": 3}
        await RedisCache.hset("testhash", payload)
        await RedisCache.hdel("testhash", ["testKey1", "testKey2"])
        # FakeRedis return response of keys() in bytes while aioredis return string.
        self.assertEqual(await RedisCache.hkeys("testhash"), [b"testKey3"])

    async def test_hkeys(self):
        payload = {"testKey1": 1, "testKey2": 2}
        await RedisCache.hset("testhash", payload)
        # FakeRedis return response of keys() in bytes while aioredis return string.
        self.assertEqual(await RedisCache.hkeys("testhash"), [b"testKey1", b"testKey2"])

    async def test_lpush(self):
        payload = [1, 2, 3]
        await RedisCache.lpush("testkey", payload)
        # FakeRedis return response of keys() in bytes while aioredis return string.
        self.assertEqual(await RedisCache.lrange("testkey", 0, -1), [b"3", b"2", b"1"])

    async def test_rpush(self):
        payload = [1, 2, 3]
        await RedisCache.rpush("testkey", payload)
        # FakeRedis return response of keys() in bytes while aioredis return string.
        self.assertEqual(await RedisCache.lrange("testkey", 0, -1), [b"1", b"2", b"3"])

    async def test_lpop(self):
        payload = [1, 2, 3]
        await RedisCache.rpush("testkey", payload)
        # FakeRedis return response of keys() in bytes while aioredis return string.
        self.assertEqual(await RedisCache.lpop("testkey"), b"1")

    async def test_lrange(self):
        payload = [1, 2, 3, 4, 5]
        await RedisCache.rpush("testkey", payload)
        # FakeRedis return response of keys() in bytes while aioredis return string.
        self.assertEqual(await RedisCache.lrange("testkey", 0, 1), [b"1", b"2"])

    async def test_delete_by_prefix(self):
        payload = {"testKey1": "testValue1", "testKey2": "testValue2"}
        await RedisCache.mset(payload)
        result = await RedisCache.delete_by_prefix("testKey")
        self.assertEqual(result, 2)

    async def test_zadd(self):
        await RedisCache.zadd("testkey", {"value1": 1, "value2": 2, "value3": 3})
        result = await RedisCache.zrange("testkey", 0, -1)
        self.assertEqual(result, [b"value1", b"value2", b"value3"])

    async def test_zrange(self):
        await RedisCache.zadd(
            "testkey", {"value1": 1, "value2": 2, "value3": 3, "value4": 4, "value5": 5}
        )
        result = await RedisCache.zrange("testkey", 1, 3)
        self.assertEqual(result, [b"value2", b"value3", b"value4"])

    async def test_zpopmin(self):
        await RedisCache.zadd(
            "testkey", {"value1": 1, "value2": 2, "value3": 3, "value4": 4, "value5": 5}
        )
        result = await RedisCache.zpopmin("testkey")
        self.assertEqual(result, [(b"value1", 1.0)])

    async def test_mset_with_expire(self):
        payload = {"testKey1": "testValue1", "testKey2": {"testKey22": "testValue22"}}
        await RedisCache.mset_with_expire(payload, 3)
        # FakeRedis return response of keys() in bytes while aioredis return string.
        self.assertEqual(
            await RedisCache.keys(),
            [b"service:base:testKey1", b"service:base:testKey2"],
        )
        await asyncio.sleep(4)
        self.assertEqual(await RedisCache.keys(), [])

    async def test_expire(self):
        result = await RedisCache.expire("testKey", 3)
        self.assertEqual(result, 0)
        await RedisCache.set("testKey", "testValue")
        result = await RedisCache.expire("testKey", 3)
        self.assertEqual(result, 1)
        await asyncio.sleep(4)
        self.assertEqual(await RedisCache.get("testKey"), None)

    async def test_is_key_exist(self):
        self.assertEqual(await RedisCache.is_key_exist("testKey"), 0)
        await RedisCache.set("testKey", "testValue")
        self.assertEqual(await RedisCache.is_key_exist("testKey"), 1)

    async def test_is_value_in_set(self):
        self.assertEqual(await RedisCache.is_value_in_set("testKey", "testValue3"), 0)
        await RedisCache.sadd("testKey", "testValue1", "testValue2")
        self.assertEqual(await RedisCache.is_value_in_set("testKey", "testValue3"), 0)
        await RedisCache.sadd("testKey", "testValue3")
        self.assertEqual(await RedisCache.is_value_in_set("testKey", "testValue3"), 1)
