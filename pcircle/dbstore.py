from __future__ import print_function
import os
import sys
import os.path
import sqlite3
import cPickle as pickle
from collections import deque
from fdef import FileItem, FileChunk, ChunkSum
from pcircle.utils import getLogger

__author__ = 'Feiyi Wang'

"""
Adapting SQL to use with workq.

API summary:

    get(self, key=None) - get single object
    put(self, obj)  - put single object
    mget(self, N) - get multiple objects
    mput(self, objs) - put multiple objects
    delete(self, obj)  - delete single object
    mdel(self, objs) - delete multiple objects
    size()  - length

    # these two operations are dangerous
    # and not encouraged

    pop(self) - get and remove one object
    popn(self, N) - get and remove multiple objects



These are the two sizes we track:

    qsize - the length of the queue/db
    fsize - the total file size in the queue/db
"""

DB_BUFSIZE = 10000


class DbStore(object):
    def __init__(self, dbname,
                 resume=False):

        self.dbname = dbname
        self.conn = None
        self.logger = getLogger(__name__)

        # debug, dbstore doesn't have to be tied with rank.
        # so, it is set to be empty
        # self.d = {"rank" : "rank %s" % self.rank}
        self.d = {"rank": ''}

        try:
            self.conn = sqlite3.connect(dbname)
        except sqlite3.OperationalError as e:
            self.logger.error(e, extra=self.d)
            sys.exit(1)
        self.cur = self.conn.cursor()
        self.resume = resume
        self.count = 0
        self.totalsz = 0
        self.fsize = 0
        self.qsize = 0
        self.workq = deque()
        self.workq_cnt = 0

        if not resume:
            self.conn.execute("DROP TABLE IF EXISTS workq")
            self.conn.execute("DROP TABLE IF EXISTS backup")
            self.conn.execute("DROP TABLE IF EXISTS checkpoint")
            self.conn.execute("CREATE TABLE workq (id INTEGER PRIMARY KEY, work BLOB)")
            self.conn.execute("CREATE TABLE backup (id INTEGER PRIMARY KEY, work BLOB)")
            self.conn.execute("CREATE TABLE checkpoint(qsize, fsize)")
            self.conn.execute("INSERT INTO checkpoint values(?, ?)",
                              (self.qsize, self.fsize))
            self.conn.execute("INSERT INTO backup (work) values(?)",
                              (sqlite3.Binary(str(0)),))
            self.conn.commit()
        else:
            self.recalibrate()

        self.logger.debug("Connected to %s" % self.dbname, extra=self.d)

    def _restore_from_backup(self):
        self.cur.execute("SELECT work FROM backup WHERE id=1")
        pdata = self.cur.fetchone()[0]
        obj = pickle.loads(str(pdata))
        self.cur.execute("INSERT INTO workq (work) VALUES (?)", (pdata,))
        self.tracksize(self.cur, obj)

    def recalibrate(self):
        # FIXME: which one to use, from checkpoint or compute?
        """
        try:
            self.cur.execute("SELECT COUNT(*) FROM workq")
            self.qsize = self.cur.fetchone()[0]
        except sqlite3.OperationalError as e:
            self.qsize = 0
        """
        try:
            self.cur.execute("SELECT * FROM checkpoint")
            self.qsize, self.fsize = self.cur.fetchone()
        except sqlite3.OperationalError as e:
            self.qsize = 0

        # we do restore_db() after looking up the checkpoint table
        # as _restore_from_backup() will try to update checkpoint table
        # with qsize and fsize since we don't have that information at this point
        # do this at the beginning will mess up the count

        #self._restore_from_backup()

    def size(self):
        return self.qsize

    def tracksize(self, cur, obj, op="plus"):
        if op == "plus":
            self.qsize += 1
            self.fsize += self._obj_size(obj)
        elif op == "minus":
            self.qsize -= 1
            self.fsize -= self._obj_size(obj)

        #cur.execute("UPDATE checkpoint SET qsize=?, fsize=?", (self.qsize, self.fsize))

    def put(self, obj):
        # should it be a transaction?
        pdata = sqlite3.Binary(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL))
        with self.conn:
            self.conn.execute("INSERT INTO workq (work) VALUES (?)", (pdata,))
            self.tracksize(self.conn, obj)
            cur.execute("UPDATE checkpoint SET qsize=?, fsize=?", (self.qsize, self.fsize))

    # alias append to match list
    append = put

    def err_and_exit(self, msg):
        if self.circle.rank == 0:
            self.logger.error(msg)
            self.circle.exit(0)

    def mput(self, objs):
        with self.conn:
            for obj in objs:
                pdata = sqlite3.Binary(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL))
                self.conn.execute("INSERT INTO workq (work) VALUES (?)", (pdata,))
                self.tracksize(self.conn, obj)

            self.cur.execute("UPDATE checkpoint SET qsize=?, fsize=?", (self.qsize, self.fsize))
        self.logger.debug("%s objs inserted to %s" % (len(objs), self.dbname), extra=self.d)

    # alias extend to match list
    extend = mput

    @staticmethod
    def _obj_size(obj):
        if isinstance(obj, FileItem):
            return 0
        elif isinstance(obj, FileChunk) or isinstance(obj, ChunkSum):
            return obj.length
        else:
            return 0
            #raise ValueError("Can't recognize %s" % obj)

    def _objs_size(self, objs):
        size = 0
        for obj in objs:
            size += self._obj_size(obj)
        return size

    def first(self):
        return self.mget(1)

    get = first

    def mget(self, n):
        objs = []
        size = 0
        if n > self.qsize:
            n = self.qsize
        with self.conn:
            for row in self.conn.execute("SELECT work FROM workq LIMIT (?) OFFSET 0", (n,)):
                obj = pickle.loads(str(row[0]))
                objs.append(obj)
                size += self._obj_size(obj)

        return objs, size

    def mdel(self, n, size=0):
        if n > self.qsize:
            n = self.qsize
        with self.conn:
            cur = self.conn.cursor()
            cur.execute("DELETE FROM workq WHERE id in (SELECT id FROM workq LIMIT (?) OFFSET 0)",
                        (n,))
            self.qsize -= n
            self.fsize -= size
            # update checkpoint table
            cur.execute("UPDATE checkpoint SET qsize=?, fsize=?", (self.qsize, self.fsize))

    def __getitem__(self, idx):
        if idx > self.qsize:
            return None
        self.cur.execute("SELECT work FROM workq LIMIT 1 OFFSET ?", (idx,))
        data = str(self.cur.fetchone()[0])
        return pickle.loads(data)

    def cleanup(self):
        if os.path.exists(self.dbname):
            self.conn.close()
            os.remove(self.dbname)

    def pop(self):
        obj = None
        rawobj = None
        if self.qsize == 0:
            return None
        with self.conn:
            self.cur.execute("SELECT work FROM workq LIMIT 1 OFFSET 0")
            rawobj = self.cur.fetchone()[0]
            obj = pickle.loads(str(rawobj))
            self.cur.execute("DELETE FROM workq WHERE id = (SELECT id FROM workq LIMIT 1 OFFSET 0)")
            self.cur.execute("UPDATE backup SET work=? WHERE id=1", (rawobj,))
            self.tracksize(self.cur, obj, op="minus")
            self.cur.execute("UPDATE checkpoint SET qsize=?, fsize=?", (self.qsize, self.fsize))
        return obj

    def __len__(self):
        return self.qsize
