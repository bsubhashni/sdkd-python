from Queue import Queue
from threading import Lock

from couchbase import Couchbase

class BucketPool(object):
    def __init__(self):
        self._q = Queue()
        self.count = 0

    def get(self):
        return self._q.get()

    def put(self, cb):
        self._q.put(cb)

class ConnectionPool(object):
    """
    Singleton connection pool
    """
    buckets = {}
    lock = Lock()

    """
    Pool size. Negative number indicates an object per allocation
    """
    POOL_SIZE = -1
    CONNCACHE = None

    @classmethod
    def allocate_instance(self, **kwargs):
        if self.CONNCACHE:
            kwargs['conncache'] = self.CONNCACHE
        self.lock.acquire()
        key = repr(kwargs['bucket'])
        pool = self.buckets.setdefault(key, BucketPool())
        try:
            if self.POOL_SIZE < 0 or pool.count < self.POOL_SIZE:

                pool.count += 1
                cb = Couchbase.connect(**kwargs)
                pool.put(cb)

            assert pool.count
        finally:
            self.lock.release()



        return pool

    @classmethod
    def release_instance(self, pool):
        self.lock.acquire()
        if pool.count:
            pool.get()
            pool.count -= 1
        self.lock.release()
