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
        self.session = cluster.connect(kspace)

    def _prepare_statements(self):
        pass

    def update_user(self, username, params):
        pass




