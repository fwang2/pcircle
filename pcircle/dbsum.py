__author__ = 'f7b'

import sys
import sqlite3
import hashlib
from cStringIO import StringIO
from pcircle.utils import getLogger


class DbSum(object):

    def __init__(self, dbname):

        self.dbname = dbname
        self.conn = None
        self.logger = getLogger(__name__)
        self.blocks = 26214
        self._size = 0

        # debug, dbstore doesn't have to be tied with rank.
        # so, it is set to be empty
        # self.d = {"rank" : "rank %s" % self.rank}
        self.d = {"rank": ''}

        try:
            self.conn = sqlite3.connect(dbname)
            self.conn.execute("DROP TABLE IF EXISTS chksums")
            self.conn.execute("CREATE TABLE chksums (path TEXT, sha1 TEXT)")
            self.conn.commit()

        except sqlite3.OperationalError as e:
            self.logger.error(e, extra=self.d)
            sys.exit(1)
        self.cur = self.conn.cursor()

    def put(self, chksum):
        with self.conn:
            self.conn.execute("INSERT INTO chksums VALUES (?, ?)", (chksum.path(), chksum.digest))
            self._size += 1

    def fsum(self):
        """  Checksum algorithm:
        layout # of block signatures sequentially in a "tmp",
        26214 in this case. we calculate a SHA1 signature on this many
        of blocks; this SHA1 signature is written to "buf" another buf.
        then tmp buffer is reinitialized.

        The reason we use two buffers is to keep the overall memory down.
        This number (26214) is consistent with fsum.py: read_in_blocks()
        nothing magic about this number, however if you change one, you have to
        change another as well to keep the final checksum consistent.

        The fsum.py should be faster, but its memory requirement is also bigger
        and depending on the dataset. This version, however, is bounded.
        """
        idx = 0
        buf = StringIO()
        tmp = StringIO()
        cursor = self.conn.execute("SELECT sha1, path FROM chksums ORDER BY path")
        for row in cursor:
            tmp.write(row[0])
            idx += 1
            if idx % self.blocks == 0 or idx == self._size:
                buf.write(hashlib.sha1(tmp.getvalue()).hexdigest())
                tmp = StringIO()
            # self.logger.info("%s - %s" %(row[1], row[0]), extra=self.d)
        return hashlib.sha1(buf.getvalue()).hexdigest()

    def size(self):
        return self._size