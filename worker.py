import sys
from rq import Connection, Worker

import kv

# Provide queue names to listen to as arguments to this script,
# similar to rq worker
with Connection(kv.KeyValue.gi().redis):
    qs = sys.argv[1:] or ['default']

    w = Worker(qs)
    w.work()
