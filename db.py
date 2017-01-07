import os

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class CassandraConfigException(Exception):
    pass

class Cassandra(object):
    
    def __init__(self):
        try:
            cpoints = os.environ['CASS_CONTACTPOINTS'].split(',')
            uname = os.environ['CASS_UNAME']
            passw = os.environ['CASS_PASS']
            kspace = os.environ['CASS_KEYSPACE']
        except IndexError:
            raise CassandraConfigException(
                "No Cassandra configuration found"
            )

        auth_provider = PlainTextAuthProvider(username=uname, 
                                              password=passw)
        self.cluster = Cluster(cpoints, auth_provider=auth_provider)
        self.session = self.cluster.connect(kspace)

    def _prepare_statements(self):
        pass

    def user_update(self, username, params):
        stmt = self.session.prepare("""
            UPDATE users SET {}
            WHERE username = ?
            """.format(', '.join(["{} = ?".format(p) for p in params]))
        )
        print(stmt)
        vals = list(params.values())
        vals.append(username)

        self.session.execute(stmt, vals)


if __name__ == '__main__':
    c = Cassandra()

    c.user_update('mescam', {'bio': 'to ja', 'password': 'niety'})




