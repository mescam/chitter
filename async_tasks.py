from redis import Redis
from rq import Queue
import kv
import db

q = Queue(connection=kv.KeyValue.gi().redis)

def _count_likes(username, uuid):
    db.Cassandra.gi().likes_count_update(username, uuid)
    return True

def count_likes(username, uuid):
    return q.enqueue(_count_likes, username, uuid)
