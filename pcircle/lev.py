from __future__ import print_function
import plyvel
import cPickle as pickle
import time
import os.path
import shutil
import logging
import sys

from pqueue import BaseQueue
from fdef import FileItem, FileChunk
from utils import logging_init

__author__ = 'f7b'

"""
Adapting KV store to use with workq.

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
from globals import G
logger = logging.getLogger("lev")
logging_init(logger, G.loglevel)

class LevStore:
    def __init__(self, dbname, resume=False):

        self.dbname = dbname
        self.fsize = 0
        self.qsize = 0

        if not resume and os.path.exists(dbname):
            shutil.rmtree(dbname)

        logger.info("Creating %s" % dbname)
        try:
            self.db = plyvel.DB(self.dbname, create_if_missing = True)
        except IOError as e:
            logger.error("%s" % e.message)
            sys.exit(1)

        # have no better way to get count of key
        # but to do a brute force iteration
        if resume:
            self.recalibrate()

    def recalibrate(self):
        """
            recalculate both qsize and fsize
        """
        self.qsize = 0
        self.fsize = 0
        for k, v in self.db:
            self.qsize += 1
            fchunk = pickle.loads(v)
            self.fsize += fchunk.length

    def count_keys(self):
        count = 0
        for k, v in self.db:
            count += 1
        return count


    def mget(self, N):
        """ Return N object in the queue, note that
            this is different from popn() which will alter
            the state of the queue
        """
        i = 0
        objs = []
        for k, v in self.db:
            objs.append(pickle.loads(v))
            if i == N:
                break
            else:
                i += 1

        return objs


    def mdel(self, objslist):
        """ objlist is a list of FileItem/FileChunk object
         this method will alter the state of the queue, which
         is comparable to popn() - the difference is the input here
         is a list of objects, while the popn() will remove the first N items
        """
        wb = self.db.write_batch()
        for obj in objslist:
            wb.delete(obj.key())
            self.fsize -= self.obj_size(obj)
        wb.write()
        self.qsize -= len(objslist)

    def delete(self, obj):

        self.db.delete(obj.key())
        self.qsize -= 1
        self.fsize -= self.obj_size(obj)

    def put(self, obj):
        """ push single object
        """
        self.db.put(obj.key(), pickle.dumps(obj))
        self.qsize += 1
        self.fsize += self.obj_size(obj)


    def mput(self, objs):
        """ push a list of objects
        """
        if not isinstance(objs, list):
            raise TypeError("Must be a list type")
        wb = self.db.write_batch()

        for obj in objs:
            wb.put(obj.key(), pickle.dumps(obj))
            self.fsize += self.obj_size(obj)

        wb.write()
        self.qsize += len(objs)

    def obj_size(self, obj):
        if isinstance(obj, FileItem):
            return 0
        elif isinstance(obj, FileChunk):
            return obj.length
        else:
            raise ValueError("Can't recognize %s" % obj)


    def pop(self):
        """
        STATE ALTER: remove first item
        """
        ret = self.popn(1)
        if len(ret) == 0:
            return None
        else:
            return ret[0]

    def popn(self, N):
        """
            STATE ALTER: remove first N items from the queue
        """
        ret = []
        wb = self.db.write_batch()
        for key, value in self.db:
            fdata = pickle.loads(value)
            ret.append(fdata)
            self.qsize -= 1
            self.fsize -= self.obj_size(fdata)
            wb.delete(key)
            if len(ret) == N: break

        wb.write()

        return ret


    def size(self):
        return self.qsize

    def sync(self):
        self.db.close()
        self.db = plyvel.DB(self.dbname)

    def cleanup(self):
        if self.db and not self.db.closed:
            self.db.close()

        try:
            plyvel.destroy_db(self.dbname)
        except IOError as e:
            print(e.message)

        logger.info("Cleaning up %s" % self.dbname)


def test_basics():
    import shutil
    import os.path

    dbname = "/tmp/test.db"
    if os.path.exists(dbname):
        shutil.rmtree(dbname)
    db = LevStore(dbname)

    db.put(FileItem("/tmp"))
    db.put(FileItem("/tmp/1"))
    assert db.size() == 2
    db.pop()
    assert db.size() == 1
    db.pop()
    db.pop()
    assert db.size() == 0

    objs = [FileItem("/1/2"), FileItem("/3/4")]
    db.pushn(objs)
    assert db.size() == 2

    print("Unit tests passed")

    db.cleanup()


def batch_inject(db, load, batchsize):
    flist = []
    for i in xrange(load):
        flist.append(FileItem("/tmp/%s" % i))
        if len(flist) == batchsize:
            db.pushn(flist)
            del flist[:]


def single_inject(work, cleanup=True):
    import os.path, shutil

    dbname = "/tmp/test.db"
    if os.path.exists(dbname):
        shutil.rmtree(dbname)
    db = LevStore(dbname)
    ts = time.time()
    for i in xrange(work):
        db.push(FileItem("/tmp/%s" % i))
    te = time.time()
    if cleanup:
        db.cleanup()
    du = te - ts
    rate = work / du
    print("Load = %d, took = %0.2f, rate = %d" % (work, du, rate))
    return db

def test_single_inject():
    for work in [10000, 100000, 1000000, 10000000]:
        single_inject(work)


def test_batch_inject():
    import os.path
    import shutil

    for bs in [1, 10, 100, 1000, 10000, 100000, 1000000]:
        dbname = "/tmp/test.db"
        if os.path.exists(dbname):
            shutil.rmtree(dbname)
        db = LevStore(dbname)

        work = 10000000
        ts = time.time()
        batch_inject(db, work, bs)
        te = time.time()
        db.cleanup()
        du = te - ts
        rate = work / du

        print("Load = %d, batch = %d, took = %0.2f, rate = %d" % (work, bs, du, rate))

def test_count_keys(load):
    db = single_inject(load, cleanup=False)
    ts = time.time()
    count = db.count_keys()
    te = time.time()
    print("key count = %d, took %.2f seconds" (count, te-ts))
