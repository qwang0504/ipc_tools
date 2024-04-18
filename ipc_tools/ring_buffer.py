from multiprocessing import RawArray, RawValue, RLock
from .queue_like import QueueLike
from typing import Optional
import numpy as np
from numpy.typing import NDArray, ArrayLike, DTypeLike
import time
from queue import Empty
from multiprocessing_logger import Logger

# TODO make a buffer that blocks instead of overflowing 
# TODO write an Object abstract class with a serialization to
# numpy array function

class RingBuffer(QueueLike):
    '''
    Simple circular buffer implementation, with the following features:
    - when the buffer is full it will overwrite unread content (overflow)
    - trying to get item from empty buffer can be either blocking (default) or non blocking (return None)
    - only one process can access the buffer at a time, writing and reading share the same lock
    - to send multiple fields with heterogeneous type, one can use numpy's structured arrays
    '''

    def __init__(
            self,
            num_items: int,
            item_shape: ArrayLike,
            data_type: DTypeLike,
            t_refresh: float = 1e-6,
            copy: bool = False,
            name: str = '',
            logger: Optional[Logger] = None
        ):
        
        self.item_shape = np.asarray(item_shape)
        self.element_type = np.dtype(data_type)
        self.t_refresh = t_refresh
        self.copy = copy
        self.name = name
        self.logger = logger
        self.local_logger = None

        # account for empty slot
        self.num_items = num_items + 1 
        self.item_num_element = int(np.prod(self.item_shape))
        self.element_byte_size = self.element_type.itemsize 
        self.total_size =  self.item_num_element * self.num_items
        
        self.lock = RLock()
        self.read_cursor = RawValue('I',0)
        self.write_cursor = RawValue('I',0)
        self.num_lost_item = RawValue('I',0)
        self.data = RawArray('B', self.total_size*self.element_byte_size) 

    def init_logger(self):
        if (self.logger is not None) and (self.local_logger is None):
            self.logger.configure_emitter()
            self.local_logger = self.logger.get_logger(self.name)
        
    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[NDArray]:
        '''return buffer to the current read location'''

        if timeout is None:
            # stay compliant with the Queue interface
            timeout = float('inf')

        if block:
            array = None
            deadline = time.monotonic() + timeout

            while (array is None): 
                
                if time.monotonic() > deadline:
                    raise Empty
                
                array = self.get_noblock()
                if array is None:
                    time.sleep(self.t_refresh)

            return array
        
        else:
            return self.get_noblock()

    def get_noblock(self) -> Optional[NDArray]:
        '''return data at the current read location'''
        
        t_start = time.perf_counter_ns() * 1e-6

        with self.lock:

            t_lock_acquired = time.perf_counter_ns() * 1e-6

            if self.empty():
                raise Empty

            if self.copy:
                element = np.frombuffer(
                    self.data, 
                    dtype = self.element_type, 
                    count = self.item_num_element,
                    offset = self.read_cursor.value * self.item_num_element * self.element_byte_size # offset should be in bytes
                ).copy()
            else:
                element = np.frombuffer(
                    self.data, 
                    dtype = self.element_type, 
                    count = self.item_num_element,
                    offset = self.read_cursor.value * self.item_num_element * self.element_byte_size # offset should be in bytes
                )
            self.read_cursor.value = (self.read_cursor.value  +  1) % self.num_items

            t_lock_released = time.perf_counter_ns() * 1e-6

        if self.local_logger:
            self.local_logger.info(f'get, {t_start}, {t_lock_acquired}, {t_lock_released}')

        # this seems to be necessary to give time to other workers to get the lock 
        time.sleep(self.t_refresh)
        
        return element.reshape(self.item_shape)
    
    def put(self, element: ArrayLike, block: Optional[bool] = True, timeout: Optional[float] = None) -> None:
        '''
        Return data at the current write location.
        block and timeout are there for compatibility with the Queue interface, but 
        are ignored since the ring buffer overflows by design.  
        '''

        t_start = time.perf_counter_ns() * 1e-6

        # convert to numpy array
        arr_element = np.asarray(element, dtype = self.element_type)

        with self.lock:

            t_lock_acquired = time.perf_counter_ns() * 1e-6

            buffer = np.frombuffer(
                self.data, 
                dtype = self.element_type, 
                count = self.item_num_element,
                offset = self.write_cursor.value * self.item_num_element * self.element_byte_size # offset should be in bytes
            )

            # if the buffer is full, overwrite the next block
            if self.full():
                self.read_cursor.value = (self.read_cursor.value  +  1) % self.num_items
                self.num_lost_item.value += 1

            # write flattened array content to buffer (a copy is made)
            buffer[:] = arr_element.ravel()

            # update write cursor value
            self.write_cursor.value = (self.write_cursor.value  +  1) % self.num_items

            t_lock_released = time.perf_counter_ns() * 1e-6

        if self.local_logger:
            self.local_logger.info(f'put, {t_start}, {t_lock_acquired}, {t_lock_released}')

        # this seems to be necessary to give time to other workers to get the lock 
        time.sleep(self.t_refresh)

    def full(self):
        ''' check if buffer is full '''
        return self.write_cursor.value == ((self.read_cursor.value - 1) % self.num_items)

    def empty(self):
        ''' check if buffer is empty '''
        return self.write_cursor.value == self.read_cursor.value

    def qsize(self):
        ''' Return number of items currently stored in the buffer '''
        return (self.write_cursor.value - self.read_cursor.value) % self.num_items
    
    def close(self):
        pass

    def clear(self):
        '''clear the buffer'''
        self.write_cursor.value = self.read_cursor.value

    def view_data(self):
        num_items = self.write_cursor.value - self.read_cursor.value
        num_element_stored = self.item_num_element * num_items

        stored_data = np.frombuffer(                
            self.data, 
            dtype = self.element_type, 
            count = num_element_stored,
            offset = self.read_cursor.value * self.item_num_element * self.element_byte_size # offset should be in bytes
        ).reshape(np.concatenate(((num_items,) , self.item_shape)))

        return stored_data
    
    def __str__(self):
        
        reprstr = (
            f'capacity: {self.num_items}\n' +
            f'item shape: {self.item_shape}\n' +
            f'data type: {self.element_type}\n' +
            f'size: {self.qsize()}\n' +
            f'read cursor position: {self.read_cursor.value}\n' + 
            f'write cursor position: {self.write_cursor.value}\n' +
            f'lost item: {self.num_lost_item.value}\n' +
            f'buffer: {self.data}\n' + 
            f'{self.view_data()}\n'
        )

        return reprstr
        
