"""
   Implementation of a simple cache that holds
   certain number of elements and removes excess
   based on LRU
"""
import collections
import threading

DEFAULT_CAPACITY=200

class Cache:
    """
    simple lru cache
    """

    def __init__(self, capacity=DEFAULT_CAPACITY):
        """
        :type capacity: :obj:`int`
        :arg capacity: cache size
        """
        self.capacity = capacity
        self.cache    = collections.OrderedDict()
        self.lock     = threading.Lock()

    def get(self,key):
        """
        :type key: :obj:`obj`
        """
        with self.lock:
            try:
                value = self.cache.pop(key)
                self.cache[key]=value
                return value
            except KeyError:
                return None

    def put(self,key,value):
        """
        :type key: :obj:`obj`
        :arg kay: key

        :type value: :obj:`obj`
        :arg kay: value

        """
        with self.lock:
            try:
                self.cache.pop(key)
            except KeyError:
                if len(self.cache) >= self.capacity:
                    """
                    remove first (oldest) item:
                    """
                    self.cache.popitem(last=False)
            self.cache[key]=value

    def __repr__(self):
        return self.cache.__repr__()


if __name__ == "__main__":   # pragma: no cover
    cache = Cache(3)
    cache.put("a",1)
    cache.put("b",1)
    cache.put("c",1)
    print cache
    cache.put("d",1)
    print cache

