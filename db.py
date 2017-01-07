import os

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

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
        self._q_chitts_by_following = self.session.prepare("""
            SELECT * FROM chitts_by_following
            WHERE follower = ? AND p_time = ?
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
        result = self.session.execute(self._q_user_get, [username])
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

    def chitts_by_following(self, follower, p_time):
        return self._execute(self._q_chitts_by_following,
            [follower, p_time])



if __name__ == '__main__':
    c = Cassandra()

    c.user_update('mescam', {'bio': 'to ja', 'password': 'niety'})

    print(c.user_get('mescam'))
    print(c.user_get('admin'))
    print(c.user_verify_password('mescam', 'niety'))
    print(c.user_verify_password('mescam', 'wonsz-zeczny'))

