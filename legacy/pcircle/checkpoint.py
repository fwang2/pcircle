class Checkpoint:
    def __init__(self, src, dest, workq, totalsize):
        self.totalsize = totalsize
        self.src = src
        self.dest = dest
        self.workq = workq
