from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


auth_provider = PlainTextAuthProvider(username='admin', password='admin13!')
cluster = Cluster(['node01.aws.mescam.pl'], auth_provider=auth_provider)

session = cluster.connect('test')
print session
rows = session.execute('SELECT id FROM users')
for user_row in rows:
    print user_row.id


