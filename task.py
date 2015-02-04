from abc import ABCMeta, abstractmethod

class Task(metaclass=ABCMeta):
    def __init__(self):
        self.queue = None

        return global_rank;

    @abstractmethod
    def create():
        pass

    @abstractmethod
    def process():
        pass

    @abstractmethod
    def reduce_init():
        pass

    @abstractmethod
    def reduce_op():
        pass

    @abstractmethod
    def reduce_finish():
        pass

