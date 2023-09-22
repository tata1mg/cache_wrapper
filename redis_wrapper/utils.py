from redis_wrapper.log import logger


class RedisLogger:
    logger = logger
    log_str = "Redis-Logs Method Name : {} |  Arguments : {} |  Result : {}"

    @classmethod
    def log(cls, func):
        async def inner(*args, **kwargs):
            result = await func(*args, **kwargs)
            arguments = kwargs if kwargs else args
            cls.logger.debug(cls.log_str.format(func.__name__, [arguments], result))
            return result

        return inner
