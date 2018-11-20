import math
import hashlib
import zlib
from bitarray import bitarray
from globals import G

class BFsignature():
    def __init__(self, total_chunks):
        self.total_chunks = total_chunks
        if self.total_chunks > 0:
            self.cal_m()
            #print("bf size = ",self.m)
        else:
            #log.error("Non-positive total_chunks")
            print("Non-positive total_chunks")
    
    def cal_m(self):
        self.k = - math.log(0.001) / math.log(2)
        self.k = int(self.k)
        self.m = - self.total_chunks * math.log(0.001) / (math.log(2)**2)
        self.m = int(self.m)
        self.bitarray = bitarray(self.m) 
        self.bitarray.setall(False)

    def insert_item(self, key):
        positions = self.cal_positions(key)
        for pos in positions:
            self.bitarray[pos] = True

    def or_bf(self, other_bitarray):
        self.bitarray = self.bitarray | other_bitarray

    def gen_signature(self):
        #print(self.bitarray)
        h = hashlib.sha1()
        # if bitarray is too large, might do this in sections
        h.update(self.bitarray.tobytes())
        return h.hexdigest() 

    def cal_positions(self, key):
        positions = []
        for i in range (self.k):
            hashValue = zlib.crc32(key, i) % self.m
            positions.append(hashValue)
        return positions
