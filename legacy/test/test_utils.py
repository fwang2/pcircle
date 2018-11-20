__author__ = 'f7b'

import unittest
from pcircle import utils


class Test(unittest.TestCase):
    """ Unit test for Circle """

    def test_conv_time(self):
        self.assertEquals('0.82s', utils.conv_time(0.82))
        self.assertEquals('23.00s', utils.conv_time(23))
        self.assertEquals('1m 0s', utils.conv_time(60.3))
        self.assertEquals('1h 33m', utils.conv_time(5601.33))
        self.assertEquals('6d 11h 35m', utils.conv_time(560132.33))


if __name__ == "__main__":
    unittest.main()
