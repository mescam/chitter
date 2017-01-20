import os
from redis import Redis

class KeyValue(object):
    __instance = None

    @staticmethod
    def gi():
        if KeyValue.__instance is None:
            KeyValue.__instance = KeyValue()
        return KeyValue.__instance

    def __init__(self):
        r_host = os.environ['REDIS_HOST']
        r_pass = os.environ['REDIS_PASS']
        self.redis = Redis(host=r_host, password=r_pass)
