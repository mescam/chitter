import os

from datetime import datetime
from uuid import UUID
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.util import uuid_from_time, datetime_from_uuid1
from cassandra.concurrent import execute_concurrent
from utils import parse_tags, partition_time
import async_tasks

PUBLIC_USER = '_public_'

class CassandraException(Exception):
    pass

class Cassandra(object):

    __instance = None

    @staticmethod
    def gi():
        if Cassandra.__instance is None:
            Cassandra.__instance = Cassandra()
        return Cassandra.__instance

    def __init__(self):
        print("connecting to database")
        self._connect()
        self._prepare_statements()

    def _connect(self):
        try:
            cpoints = os.environ['CASS_CONTACTPOINTS'].split(',')
            uname = os.environ['CASS_UNAME']
            passw = os.environ['CASS_PASS']
            kspace = os.environ['CASS_KEYSPACE']
        except IndexError:
            raise CassandraException(
                "No Cassandra configuration found"
            )

        auth_provider = PlainTextAuthProvider(username=uname,
                                              password=passw)
        self.cluster = Cluster(cpoints, auth_provider=auth_provider)
        self.session = self.cluster.connect(kspace)

    def _resultset(self, rs, func):
        for r in rs:
            yield func(r)

    def _execute(self, *args, **kwargs):
        i = 0
        while i < 3:
            try:
                return self.session.execute(*args, **kwargs)
            except cassandra.cluster.NoHostAvailable:
                self._connect()
                i += 1
        raise CassandraException("No host available")

    def _prepare_statements(self):
        self._q_user_get = self.session.prepare("""
            SELECT * FROM users
            WHERE username = ?
            """)
        self._q_user_verify_password = self.session.prepare("""
            SELECT password FROM users
            WHERE username = ?
            """)

        self._q_chitts_by_user_add = self.session.prepare("""
            INSERT INTO chitts_by_user
                (username, body, time, likes, p_time)
            VALUES (?, ?, ?, 0, ?)
            """)
        self._q_chitts_by_following_add = self.session.prepare("""
            INSERT INTO chitts_by_following
                (follower, username, body, time, likes, p_time)
            VALUES (?, ?, ?, ?, 0, ?)
            """)
        self._q_chitts_by_tag_add = self.session.prepare("""
            INSERT INTO chitts_by_tag
                (tag, username, body, time, likes, p_time)
            VALUES (?, ?, ?, ?, 0, ?)
            """)
        self._q_chitts_by_following = self.session.prepare("""
            SELECT username, body, time, likes
            FROM chitts_by_following
            WHERE follower = ? AND p_time = ?
            """)
        self._q_chitts_by_user = self.session.prepare("""
            SELECT username, body, time, likes
            FROM chitts_by_user
            WHERE username = ? AND p_time = ?
            """)
        self._q_chitts_by_tag = self.session.prepare("""
            SELECT username, body, time, likes
            FROM chitts_by_tag
            WHERE tag = ? AND p_time = ?
            """)

        self._q_following_add = self.session.prepare("""
            INSERT INTO following_by_user (username, following)
            VALUES (?, ?)
            """)
        self._q_following_delete = self.session.prepare("""
            DELETE
            FROM following_by_user
            WHERE username = ?
            AND following = ?
            """)
        self._q_following_by_user = self.session.prepare("""
            SELECT following
            FROM following_by_user
            WHERE username = ?
            """)

        self._q_follower_add = self.session.prepare("""
            INSERT INTO followers_by_user (username, follower)
            VALUES (?, ?)
            """)
        self._q_follower_delete = self.session.prepare("""
            DELETE
            FROM followers_by_user
            WHERE username = ?
            AND follower = ?
            """)
        self._q_followers_by_user = self.session.prepare("""
            SELECT follower
            FROM followers_by_user
            WHERE username = ?
            """)

        self._q_likes_count_by_chitt = self.session.prepare("""
            SELECT COUNT(*)
            FROM likes_by_chitt
            WHERE time = ?
            """)

        self._q_likes_by_user_add = self.session.prepare("""
            INSERT INTO likes_by_user (username, time, p_time)
            VALUES (?, ?, ?)
            """)
        self._q_likes_by_user_delete = self.session.prepare("""
            DELETE
            FROM likes_by_user
            WHERE username = ?
            AND time = ?
            AND p_time = ?
            """)
        self._q_likes_by_user = self.session.prepare("""
            SELECT username, time
            FROM likes_by_user
            WHERE username = ?
            AND p_time = ?
            """)
        self._q_likes_by_chitt_add = self.session.prepare("""
            INSERT INTO likes_by_chitt (time, username)
            VALUES (?, ?)
            """)
        self._q_likes_by_chitt_delete = self.session.prepare("""
            DELETE
            FROM likes_by_chitt
            WHERE time = ?
            AND username = ?
            """)
        self._q_likes_by_chitt = self.session.prepare("""
            SELECT username
            FROM likes_by_chitt
            WHERE time = ?
            """)

        self._q_update_likes_in_chitts_by_user = self.session.prepare("""
            UPDATE chitts_by_user
            SET likes = ?
            WHERE username = ?
            AND p_time = ?
            AND time = ?
            """)

        self._q_update_likes_in_chitts_by_following = self.session.prepare("""
            UPDATE chitts_by_following
            SET likes = ?
            WHERE follower = ?
            AND p_time = ?
            AND time = ?
            """)

        self._q_update_likes_in_chitts_by_tag = self.session.prepare("""
            UPDATE chitts_by_tag
            SET likes = ?
            WHERE tag = ?
            AND p_time = ?
            AND time = ?
            """)

        self._q_get_chitt_body = self.session.prepare("""
            SELECT body
            FROM chitts_by_user
            WHERE username = ? AND p_time = ? AND time = ?
            """)


    def user_update(self, username, params):
        stmt = self.session.prepare("""
            UPDATE users SET {}
            WHERE username = ?
            """.format(', '.join(["{} = ?".format(p) for p in params]))
        )
        print(stmt)
        vals = list(params.values())
        vals.append(username)

        self._execute(stmt, vals)

    def user_get(self, username):
        result = self._execute(self._q_user_get, [username])
        try:
            return result[0]
        except IndexError:
            return None

    def user_verify_password(self, username, password):
        result = self._execute(
            self._q_user_verify_password,
            [username]
        )
        try:
            return (result[0].password == password)
        except IndexError:
            return False

    def follow_user(self, username, user_to_follow):
        self._execute(self._q_following_add, [username, user_to_follow])
        self._execute(self._q_follower_add, [user_to_follow, username])

    def unfollow_user(self, username, user_to_unfollow):
        self._execute(self._q_following_delete, [username, user_to_unfollow])
        self._execute(self._q_follower_delete, [user_to_unfollow, username])

    def following_by_user(self, username):
        return self._resultset(
            self._execute(self._q_following_by_user, [username]),
            lambda x: x.following
        )

    def followers_by_user(self, username):
        return self._resultset(
            self._execute(self._q_followers_by_user, [username]),
            lambda x: x.follower
        )

    def chitts_by_following(self, follower, p_time):
        return self._execute(self._q_chitts_by_following,
            [follower, p_time])

    def chitts_by_user(self, username, p_time):
        return self._execute(self._q_chitts_by_user,
            [username, p_time])

    def chitts_by_tag(self, tag, p_time):
        return self._execute(self._q_chitts_by_tag,
            [tag, p_time])

    def chitts_public(self, p_time):
        return self._execute(self._q_chitts_by_following,
            [PUBLIC_USER, p_time])

    def chitt_add(self, username, body):
        current_time = datetime.now()
        time = uuid_from_time(current_time)
        p_time = partition_time(current_time)

        self._execute(self._q_chitts_by_user_add,
            [username, body, time, p_time])

        followers = self.followers_by_user(username)
        for f in followers:
            self._execute(self._q_chitts_by_following_add,
                [f, username, body, time, p_time])
        self._execute(self._q_chitts_by_following_add,
            [username, username, body, time, p_time])
        self._execute(self._q_chitts_by_following_add,
            [PUBLIC_USER, username, body, time, p_time])

        tags = parse_tags(body)
        for t in tags:
            self._execute(self._q_chitts_by_tag_add,
                [t, username, body, time, p_time])

    def like_add(self, username, chitt_author, time_string):
        time = UUID(time_string)
        p_time = partition_time(datetime_from_uuid1(time))
        self._execute(self._q_likes_by_user_add,
            [username, time, p_time])
        self._execute(self._q_likes_by_chitt_add,
            [time, username])
        async_tasks.count_likes(chitt_author, time_string)

    def like_delete(self, username, chitt_author, time_string):
        time = UUID(time_string)
        p_time = partition_time(datetime_from_uuid1(time))
        self._execute(self._q_likes_by_user_delete,
            [username, time, p_time])
        self._execute(self._q_likes_by_chitt_delete,
            [time, username])
        async_tasks.count_likes(chitt_author, time_string)

    def likes_by_user(self, username, p_time):
        return self._execute(self._q_likes_by_user,
            [username, p_time])

    def likes_by_chitt(self, time_string):
        time = UUID(time_string)
        return self._execute(self._q_likes_by_chitt,
            [time])

    def likes_count_update(self, username, time_string):
        time = UUID(time_string)
        p_time = partition_time(datetime_from_uuid1(time))
        likes = self._execute(self._q_likes_count_by_chitt,
                           [time])[0].count
        queries = []
        queries.append(
            (self._q_update_likes_in_chitts_by_user,
            (likes, username, p_time, time)))

        for follower in self.followers_by_user(username):
            queries.append(
                (self._q_update_likes_in_chitts_by_following,
                (likes, follower, p_time, time))
            )

        tags = parse_tags(
            self._execute(
                self._q_get_chitt_body,
                (username, p_time, time)
            )[0].body)

        for t in tags:
            queries.append((
                self._q_update_likes_in_chitts_by_tag,
                (likes, tag, p_time, time)))

        results = execute_concurrent(
            self.session,
            queries
        )
                


if __name__ == '__main__':
    c = Cassandra.gi()
    assert Cassandra.gi() is Cassandra.gi()
    c.user_update('mescam', {'bio': 'to ja', 'password': 'niety'})

    print(c.user_get('mescam'))
    print(c.user_get('admin'))
    print(c.user_verify_password('mescam', 'niety'))
    print(c.user_verify_password('mescam', 'wonsz-zeczny'))
    print(list(c.following_by_user('jacek')))
    print(list(c.followers_by_user('kuba')))
    c.follow_user('wacek', 'kuba')
    c.follow_user('jacek', 'kuba')
    c.like_add('wacek', 'kuba', '321595b8-d5c5-11e6-8e60-2c1c6ff16c9a')
    print(list(c.likes_by_user('wacek', '2017-1')))
    print(list(c.likes_by_chitt('321595b8-d5c5-11e6-8e60-2c1c6ff16c9a')))
    c.like_delete('wacek', 'kuba', '321595b8-d5c5-11e6-8e60-2c1c6ff16c9a')
    async_tasks.count_likes('12', 'lel')
    print(list(c.chitts_public('2017-1')))
