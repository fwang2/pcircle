import unittest
import shutil
import os.path

from pcircle.dbstore import DbStore
from pcircle.fdef import FileItem


class Test(unittest.TestCase):
    """ Unit test for Circle """

    def setUp(self):
        dbname = "/tmp/test.db"
        if os.path.exists(dbname):
            os.remove(dbname)
        self.db = DbStore(dbname)
        self.db.put(FileItem("/tmp"))
        self.db.put(FileItem("/tmp/1"))
        self.db.put(FileItem("/tmp/2"))

    def tearDown(self):
        self.db.cleanup()

    def test_dbstore_put(self):
        self.assertEquals(self.db.size(), 3)

    def test_dbstore_pop(self):
        self.db.pop()

        self.assertEquals(self.db.size(), 2)
        self.db.pop()
        self.db.pop()
        self.assertEquals(self.db.size(), 0)

        # what happens when db is empty?
        self.db.pop()
        self.assertEquals(self.db.size(), 0)

    def test_dbstore_mput(self):
        objs = [FileItem("/1/2"), FileItem("/3/4")]
        self.db.mput(objs)

        self.assertEquals(self.db.size(), 5)

    def test_dbstore_mdel(self):
        self.db.mdel(3)
        self.assertEquals(self.db.size(), 0)


if __name__ == "__main__":
    unittest.main()
