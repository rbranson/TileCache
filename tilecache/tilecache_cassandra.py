#!/usr/bin/env python

import sys
import pycassa
import inspect
import pprint

if len(sys.argv) < 3:
    print "Usage: " + sys.argv[0] + " <hostname> <counts/invalidate> [layer]"
    sys.exit(2)

pool = pycassa.ConnectionPool(keyspace='TileCache', server_list=[ sys.argv[1] ])
cf = pycassa.ColumnFamily(pool, 'Tiles')

cmd = sys.argv[2]

if cmd == "counts":
    if len(sys.argv) > 3:
        layers = [ sys.argv[3] ]
    else:
        layers = dict(cf.get_range(column_count=0, filter_empty=False))

    # do this one at a time because it can take a considerable amount of time
    # to do the actual count, and we don't want to time out.
    for key in layers:
        count = cf.get_count(key)
        print key + ": " + str(count)
elif cmd == "invalidate":
    if len(sys.argv) > 3:
        cf.remove(sys.argv[3])
    else:
        print "No layer specified!"
        sys.exit(3)
else:
    print "Invalid command specified: " + cmd
    sys.exit(4)
