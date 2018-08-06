import collections
class LRU:
    """
    This class implements a simple LRU cache.
    The cache is maintained by OrderedDict. A read op will pop
    the item out first, then write it back. The net effect is to move
    entry to the end of the list. When eviction happens (by writing new
    entry to the cache), the evicted item will come from the front
    of the list.

    Optionally, callback func will be invoked on the evicted (k,v) pair
    """

    def __init__(self, capacity, callback=None):
        self.capacity = capacity
        self.cache = collections.OrderedDict()
        self.callback = callback

    def get(self, key):
        try:
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
        except KeyError:
            return -1

    def set(self, key, value):
        try:
            self.cache.pop(key)
        except KeyError:
            if len(self.cache) >= self.capacity:
                k, v = self.cache.popitem(last=False) # last in, first out
                if self.callback:
                    self.callback(k, v)
        self.cache[key] = value

    def clear(self):
        if self.callback:
            for k, v in self.cache.items():
                self.callback(k, v)
        self.cache.clear()

    def has_key(self, k):
        if k in self.cache.keys():
            return True
        else:
            return False

