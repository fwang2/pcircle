from abc import ABCMeta, abstractmethod


class BaseQueue:
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def size(self):
        pass

    @abstractmethod
    def pushn(self, obj):
        pass

    @abstractmethod
    def popn(self, n):
        pass

    @abstractmethod
    def pop(self):
        pass

    @abstractmethod
    def __getitem__(self, idx):
        pass

    @abstractmethod
    def cleanup(self):
        pass
