class ChunkSum:
    def __init__(self, filename, off_start=0, length=0):
        self.filename = filename
        self.off_start = off_start
        self.length = length
        self.digest = 0

    # FIXME: this is not Python 3 compatible
    def __cmp__(self, other):
        assert isinstance(other, ChunkSum)
        return cmp((self.filename, self.off_start, self.digest),
                   (other.filename, other.off_start, self.digest))

    def __repr__(self):
        return "-".join([str(x) for x in [self.filename, self.off_start, self.length, self.digest]])

    def __hash__(self):
        return hash(repr(self))