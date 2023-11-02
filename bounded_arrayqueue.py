
from multiprocessing import RawArray, RawValue, RLock
from typing import Optional
import numpy as np
from numpy.typing import NDArray, ArrayLike, DTypeLike
from abc import ABC, abstractmethod

class BoundedQueue:

    def __init__(self, size, maxlen):
        self.size = size
        self.maxlen = maxlen
        self.itemsize = np.prod(size)

        self.numel = Value('i',0)
        self.insert_ind = Value('i',0)
        self.data = RawArray(ctypes.c_float, int(self.itemsize*maxlen))
    
    def append(self, item) -> None:
        data_np = np.frombuffer(self.data, dtype=np.float32).reshape((self.maxlen, *self.size))
        np.copyto(data_np[self.insert_ind.value,:,:], item)
        self.numel.value = min(self.numel.value+1, self.maxlen) 
        self.insert_ind.value = (self.insert_ind.value + 1) % self.maxlen

    def get_data(self):
        if self.numel.value == 0:
            return None
        else:
            data_np = np.frombuffer(self.data, dtype=np.float32).reshape((self.maxlen, *self.size))
            return data_np[0:self.numel.value,:,:]
