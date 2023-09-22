from .cache_hosts import cache_hosts
from .wrapper import RedisWrapper


class RegisterRedis:
    @staticmethod
    def register_redis_cache(cache_config: dict, conn=None):
        for name, config in cache_config.items():
            cache_hosts[config.get("LABEL", "global")] = RedisWrapper(
                host=config.get("REDIS_HOST", "localhost"),
                port=config.get("REDIS_PORT", 6379),
                conn=conn,
            )
        return cache_hosts
