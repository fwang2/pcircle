class Chunk:
    def __init__(self, filename, off_start=0, length=0):
        self.filename = filename
        self.off_start = off_start
        self.length = length
        self.digest = 0

    # FIXME: this is not Python 3 compatible
    def __cmp__(self, other):
        assert isinstance(other, Chunk)
        return cmp((self.filename, self.off_start, self.digest),
                   (other.filename, other.off_start, self.digest))

