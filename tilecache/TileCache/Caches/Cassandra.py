# BSD Licensed, Copyright (c) 2006-2010 TileCache Contributors
#
# This is an implementation of a Cassandra backend for TileCache. Each
# layer lives on an individual row, and each tile is a column. It uses a
# combination of the zoom level and an interleaved X,Y coordinate as the
# column name, to get some semblance of disk locality for tiles that would
# appear within the same bounding box on a slippy map.
#
# It's useful for a case with a large number of layers, and each layer needs
# to be invalidated completely in a single operation. The invalidation can
# be done by simply doing a row delete in Cassandra, which is a very
# lightweight operation.
#
# You'll need a memcache to perform locking, which is sort of an optimistic
# use case. It's not dependent on this, because repeated operations would
# simply rewrite the same tile, but it does use this as a performance
# optimization for metatile renders. If memcache loses a lock, nothing actually
# breaks, so it's pointless to use something more sophisticated like ZooKeeper.
#
# To create the schema and keyspace with the "cassandra-cli":
#
#  [default@unknown] create keyspace TileCache;
#  [default@unknown] use TileCache;
#  [default@TileCache] create column family Tiles with key_validation_class='UTF8Type';
#
# To invalidate a layer with the "cassandra-cli":
#
#  [default@TileCache] del Tiles['basic'];
#
# To count the number of tiles in a layer:
#
#  [default@TileCache] count Tiles['basic'];
#  319 columns
#
# To configure:
#
#  [cache]
#  type=Cassandra
#  memcache_servers=192.168.1.1:11211,192.168.1.2:11211,192.168.1.3:11211
#  cassandra_nodes=10.1.1.1:9160,10.1.1.2:9160,10.1.1.3:9160,10.1.1.4:9160
#

from TileCache.Cache import Cache
import time
import struct

class Cassandra(Cache):
    def __init__ (self, memcache_servers = ['127.0.0.1:11211'], cassandra_nodes = ['127.0.0.1:9160'], keyspace = 'TileCache', **kwargs):
        Cache.__init__(self, **kwargs)
        import memcache
        import pycassa

        if type(memcache_servers) is str: memcache_servers = map(str.strip, memcache_servers.split(","))
        if type(cassandra_nodes) is str: cassandra_nodes = map(str.strip, cassandra_nodes.split(","))

        self.memcache = memcache.Client(memcache_servers, debug=0)
        self.pool = pycassa.ConnectionPool(keyspace=keyspace, server_list=cassandra_nodes, prefill=False)
        self.cf = pycassa.ColumnFamily(self.pool, "Tiles")
        self.pycassa = pycassa

    def getKey(self, tile):
        return "/".join(map(str, [tile.layer.name, tile.x, tile.y, tile.z]))

    def getRowKey(self, tile):
        return tile.layer.name

    def getColumnName(self, tile):
        # Big endian is chosen here because it naturally sorts with byte order, and
        # interleave2() provides z-curve ordering to colocate tiles in the same geographical
        # area on disk for better use of the OS page caching facilities.
        return struct.pack(">LL", tile.z, self.interleave2(tile.x, tile.y)) 
        
    def get(self, tile):
        key = self.getRowKey(tile)
        name = self.getColumnName(tile)

        try:
            result = self.cf.get(key, columns = [name])
            tile.data = result[name]
            return tile.data
        except self.pycassa.NotFoundException:
            return None
    
    def set(self, tile, data):
        if self.readonly: return data
        key = self.getRowKey(tile)
        name = self.getColumnName(tile)
        self.cf.insert(key, { name: data })
        return data
    
    def delete(self, tile):
        key = self.getRowKey(tile)
        name = self.getColumnName(tile)
        self.cf.delete(key)

    def attemptLock (self, tile):
        return self.memcache.add( self.getLockName(tile), "0", 
                                  time.time() + self.timeout)
    
    def unlock(self, tile):
        self.memcache.delete( self.getLockName(tile) )

    def part1by1(self, n):
        n&= 0x0000ffff
        n = (n | (n << 8)) & 0x00FF00FF
        n = (n | (n << 4)) & 0x0F0F0F0F
        n = (n | (n << 2)) & 0x33333333
        n = (n | (n << 1)) & 0x55555555
        return n

    def interleave2(self, x, y):
        return self.part1by1(x) | (self.part1by1(y) << 1)
