__author__ = 'f7b'

class CommonEqualityMixin(object):
    def __eq__(self, other):
        return (isinstance(other, self.__class__)
            and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)

class FileItem(CommonEqualityMixin):
    def __init__(self, path, st_mode=0, st_size=0, st_uid=0, st_gid=0):
        self.path = path
        self.st_mode = st_mode
        self.st_size = st_size
        self.st_uid = st_uid
        self.st_gid = st_gid


    def __str__(self):
        return ",".join([self.path, str(self.st_mode), str(self.st_size)])

    def __repr__(self):
        return "FileItem: " + self.__str__()

    def key(self):
        return self.path


class FileChunk(CommonEqualityMixin):

    def __init__(self, cmd="copy",
                 src="", dest="", offset=0, length=0):
        self.cmd = cmd
        self.src = src
        self.dest = dest
        self.offset = offset
        self.length = length

    def key(self):
        return "%s::%s" % (self.src, self.offset)

    def __repr__(self):
        return "FileChunk: " + self.__str__()

    def __str__(self):
        return ",".join([self.src, str(self.offset), str(self.length)])


class ChunkSum:
    """ make __cmp__ part of the mixin so it can be reused
    """

    def __init__(self, filename, offset=0, length=0, digest=""):
        self.filename = filename
        self.offset = offset
        self.length = length
        self.digest = digest

    def __cmp__(self, other):
        assert isinstance(other, ChunkSum)
        return cmp((self.filename, self.offset, self.length, self.digest),
                   (other.filename, other.offset, self.length, self.digest))

    def __repr__(self):
        return "-".join([str(x) for x in [self.filename, self.offset, self.length]])

    def __hash__(self):
        return hash(repr(self))

    def __str__(self):
        return self.__repr__()

    def path(self):
        return self.__repr__()