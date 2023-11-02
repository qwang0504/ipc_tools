from multiprocessing import RawArray, RawValue, RLock
from typing import Optional
import numpy as np
from numpy.typing import NDArray, ArrayLike, DTypeLike
from abc import ABC, abstractmethod

class RingBuffer(ABC):

    @abstractmethod
    def get(self):
        pass

    @abstractmethod
    def put(self):
        pass

    @abstractmethod
    def full(self):
        pass

    @abstractmethod
    def empty(self):
        pass

    @abstractmethod
    def size(self):
        pass

class OverflowRingBuffer_Locked(RingBuffer):
    '''
    If there is an overflow, unread content will be lost.
    Only one process can access the buffer at a time.
    '''

    def __init__(
            self,
            num_items: int,
            item_shape: ArrayLike,
            data_type: DTypeLike
        ):
        
        self.item_shape = item_shape
        self.element_type = np.dtype(data_type)

        # account for empty slot
        self.num_items = num_items + 1 
        self.item_num_element = int(np.prod(self.item_shape))
        self.total_size =  self.item_num_element * self.num_items
        
        self.lock = RLock()
        self.read_cursor = RawValue('I',0)
        self.write_cursor = RawValue('I',0)
        self.lost_item = RawValue('I',0)
        self.data = RawArray(self.element_type.char, self.total_size) 
        
    def get(self) -> Optional[NDArray]:
        '''return buffer to the current read location'''

        with self.lock:

            if self.empty():
                return None

            element = np.frombuffer(
                self.data, 
                dtype = self.element_type, 
                count = self.item_num_element,
                offset = self.read_cursor.value * self.item_num_element 
            )
            self.read_cursor.value = (self.read_cursor.value  +  1) % self.num_items

        return element.reshape(self.item_shape)

    def put(self, element: ArrayLike) -> None:
        '''return buffer to the current write location'''

        # convert to numpy array
        arr_element = np.asarray(element, dtype = self.element_type)

        with self.lock:

            buffer = np.frombuffer(
                self.data, 
                dtype = self.element_type, 
                count = self.item_num_element,
                offset = self.write_cursor.value * self.item_num_element
            )

            # if the buffer is full, overwrite the next block
            if self.full():
                self.read_cursor.value = (self.read_cursor.value  +  1) % self.num_items
                self.lost_item.value += 1

            # write flattened array content to buffer
            buffer[:] = arr_element.ravel()

            # update write cursor value
            self.write_cursor.value = (self.write_cursor.value  +  1) % self.num_items

    def full(self):
        ''' check if buffer is full '''
        return self.write_cursor.value == ((self.read_cursor.value - 1) % self.num_items)

    def empty(self):
        ''' check if buffer is empty '''
        return self.write_cursor.value == self.read_cursor.value

    def size(self):
        ''' Return number of items currently stored in the buffer '''
        return (self.write_cursor.value - self.read_cursor.value) % self.num_items
    
    def __str__(self):
        reprstr = (
            f'capacity: {self.num_items}\n' +
            f'item shape: {self.item_shape}\n' +
            f'data type: {self.element_type}\n' +
            f'size: {self.size()}\n' +
            f'read cursor position: {self.read_cursor.value}\n' + 
            f'write cursor position: {self.write_cursor.value}\n' +
            f'lost item: {self.lost_item.value}\n' +
            f'buffer: {self.data}\n'
        )

        return reprstr
        