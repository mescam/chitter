import os

from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.util import uuid_from_time

class CassandraException(Exception):
    pass

class Cassandra(object):
    
    def __init__(self):
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
            INSERT INTO chitts_by_user (username, body, time, likes, p_time)
            VALUES (?, ?, ?, 0, ?)
            """)
        self._q_chitts_by_following = self.session.prepare("""
            SELECT * FROM chitts_by_following
            WHERE follower = ? AND p_time = ?
            """)
        self._q_chitts_by_user = self.session.prepare("""
            SELECT * FROM chitts_by_user
            WHERE username = ? AND p_time = ?
            """)
        self._q_chitts_by_tag = self.session.prepare("""
            SELECT * FROM chitts_by_tag
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

    def add_chitt(self, username, body):
        current_time = datetime.now()
        time = uuid_from_time(current_time)
        p_time = str(current_time.isocalendar()[:2])

        self.execute(self._q_chitts_by_user_add,
            [username, body, time, p_time])

        followers = 


if __name__ == '__main__':
    c = Cassandra()

    c.user_update('mescam', {'bio': 'to ja', 'password': 'niety'})

    print(c.user_get('mescam'))
    print(c.user_get('admin'))
    print(c.user_verify_password('mescam', 'niety'))
    print(c.user_verify_password('mescam', 'wonsz-zeczny'))
    print(list(c.following_by_user('jacek')))
    print(list(c.followers_by_user('kuba')))
