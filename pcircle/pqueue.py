from Queue import PriorityQueue


class LazyQueue:

    def __init__(self):
        self._queue = PriorityQueue()

    def enq(self, item):
        self._queue.put(item)

    def deq(self):
        if self._queue.empty():
            return None
        else:
            return self._queue.get()

    def __len__(self):
        return self._queue.qsize()

    def mget(self, n):
        if n > self._queue.qsize():
            n = self._queue.qsize()
        lst = []
        for _ in xrange(n):
            lst.append(self._queue.get())

        return lst

    def extend(self, iterable):
        for ele in iterable:
            self.enq(ele)

    def append(self, item):
        self.enq(item)
