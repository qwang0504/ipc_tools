from multiprocessing import RawArray, RawValue, RLock, Value
from typing import Optional, List
import numpy as np
from numpy.typing import NDArray, ArrayLike, DTypeLike
from abc import ABC, abstractmethod
import time

# TODO make a buffer that blocks instead of overflowing 

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
    Simple circular buffer implementation, with the following features:
    - when the buffer is full it will overwrite unread content (overflow)
    - trying to get item from empty buffer can be either blocking (default) or non blocking (return None)
    - only one process can access the buffer at a time, writing and reading share the same lock
    '''

    def __init__(
            self,
            num_items: int,
            item_shape: ArrayLike,
            data_type: DTypeLike,
            t_refresh: float = 0.001
        ):
        
        self.item_shape = np.asarray(item_shape)
        self.element_type = np.dtype(data_type)
        self.t_refresh = t_refresh

        # account for empty slot
        self.num_items = num_items + 1 
        self.item_num_element = int(np.prod(self.item_shape))
        self.element_byte_size = self.element_type.itemsize 
        self.total_size =  self.item_num_element * self.num_items
        
        self.lock = RLock()
        self.read_cursor = RawValue('I',0)
        self.write_cursor = RawValue('I',0)
        self.lost_item = RawValue('I',0)
        self.data = RawArray(self.element_type.char, self.total_size) 
        
    def get(self, blocking: bool = True, timeout: float = float('inf'), copy: bool = False) -> Optional[NDArray]:
        '''return buffer to the current read location'''

        if blocking:
            array = None
            deadline = time.monotonic() + timeout

            while (array is None) and (time.monotonic() < deadline): 
                array = self.get_noblock(copy)
                if array is None:
                    time.sleep(self.t_refresh)

            return array
        
        else:
            return self.get_noblock(copy)

    def get_noblock(self, copy: bool) -> Optional[NDArray]:
        '''return buffer to the current read location'''

        with self.lock:

            if self.empty():
                return None

            if copy:
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
                offset = self.write_cursor.value * self.item_num_element * self.element_byte_size # offset should be in bytes
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
            f'size: {self.size()}\n' +
            f'read cursor position: {self.read_cursor.value}\n' + 
            f'write cursor position: {self.write_cursor.value}\n' +
            f'lost item: {self.lost_item.value}\n' +
            f'buffer: {self.data}\n' + 
            f'{self.view_data()}\n'
        )

        return reprstr
        

class MultiRingBuffer_Locked(RingBuffer):
    '''
    Circular buffer backed by multiple synchronized ctypes array sharing the same 
    read and write pointers and lock
    '''

    def __init__(
            self,
            num_items: int,
            item_shape: List[ArrayLike],
            data_type: List[DTypeLike],
            t_refresh: float = 0.001
        ):
        
        # account for empty slot
        self.num_items = num_items + 1 

        self.t_refresh = t_refresh
        self.num_array = len(item_shape)
        if len(data_type) != self.num_array:
            raise ValueError("item_shape and data_type should have the same number of elements")

        self.item_shape = []
        self.element_type = []
        self.item_num_element = []
        self.element_byte_size = []
        self.total_size = []
        self.data = []
        for i in range(self.num_array):
            self.item_shape.append(np.asarray(item_shape[i]))
            self.element_type.append(np.dtype(data_type[i]))
            self.item_num_element.append(int(np.prod(self.item_shape[i])))
            self.element_byte_size.append(self.element_type[i].itemsize) 
            self.total_size.append(self.item_num_element[i] * self.num_items)
            self.data.append(RawArray(self.element_type[i].char, self.total_size[i]))

        self.lock = RLock()
        self.read_cursor = RawValue('I',0)
        self.write_cursor = RawValue('I',0)
        self.lost_item = RawValue('I',0)

    def get(self, blocking: bool = True, timeout: float = float('inf'), copy: bool = False) -> Optional[NDArray]:
        '''return buffer to the current read location'''

        if blocking:
            array = []
            for i in range(self.num_array):
                array.append(None)

            deadline = time.monotonic() + timeout

            while any([a is None for a in array]) and (time.monotonic() < deadline): 
                array = self.get_noblock(copy)
                if any([a is None for a in array]):
                    time.sleep(self.t_refresh)

            return array
        
        else:
            return self.get_noblock(copy)

    def get_noblock(self, copy: bool) -> List[Optional[NDArray]]:
        '''return buffer to the current read location'''

        with self.lock:

            if self.empty():
                res = []
                for i in range(self.num_array):
                    res.append(None)
                return res

            element = []
            if copy:
                for i in range(self.num_array):
                    element.append(
                        np.frombuffer(
                            self.data[i], 
                            dtype = self.element_type[i], 
                            count = self.item_num_element[i],
                            offset = self.read_cursor.value * self.item_num_element[i] * self.element_byte_size[i] # offset should be in bytes
                        ).copy().reshape(self.item_shape[i])
                    )
            else:
                for i in range(self.num_array):
                    element.append(
                        np.frombuffer(
                            self.data[i], 
                            dtype = self.element_type[i], 
                            count = self.item_num_element[i],
                            offset = self.read_cursor.value * self.item_num_element[i] * self.element_byte_size[i] # offset should be in bytes
                        ).reshape(self.item_shape[i])
                    )

            self.read_cursor.value = (self.read_cursor.value  +  1) % self.num_items

        return element
    
    def put(self, element: List[ArrayLike]) -> None:
        '''return buffer to the current write location'''

        if len(element) != self.num_array:
            raise ValueError(f"element should have {self.num_array} elements")
        
        arr_element = []
        for i in range(self.num_array):

            # convert to numpy array
            arr_element.append(np.asarray(element[i], dtype = self.element_type[i]))

            # check dtype
            if arr_element[i].dtype != self.element_type[i]:
                raise ValueError(f"element {i} has the wrong dtype, should be {self.element_type[i]}")
            
            # check shape
            if any([True for s0,s1 in zip(arr_element[i].shape, self.item_shape[i]) if s0 != s1]):
                raise ValueError(f"element {i} has the wrong shape, should be {self.item_shape[i]}")
        
        with self.lock:

            for i in range(self.num_array):

                buffer = np.frombuffer(
                    self.data[i], 
                    dtype = self.element_type[i], 
                    count = self.item_num_element[i],
                    offset = self.write_cursor.value * self.item_num_element[i] * self.element_byte_size[i] # offset should be in bytes
                )

                # write flattened array content to buffer
                buffer[:] = arr_element[i].ravel()

            # if the buffer is full, overwrite the next block
            if self.full():
                self.read_cursor.value = (self.read_cursor.value  +  1) % self.num_items
                self.lost_item.value += 1

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
    
    def clear(self):
        '''clear the buffer'''
        self.write_cursor.value = self.read_cursor.value

    def view_data(self):
        
        num_items = self.write_cursor.value - self.read_cursor.value
        stored_data = []
        for i in range(self.num_array):      
            num_element_stored = self.item_num_element[i] * num_items

            stored_data.append(
                np.frombuffer(                
                    self.data[i], 
                    dtype = self.element_type[i], 
                    count = num_element_stored,
                    offset = self.read_cursor.value * self.item_num_element[i] * self.element_byte_size[i] # offset should be in bytes
                ).reshape(np.concatenate(((num_items,) , self.item_shape[i] )))
            )
        
        return stored_data
    
    def __str__(self):
        
        reprstr = (
            f'capacity: {self.num_items}\n' +
            f'item shape: {self.item_shape}\n' +
            f'data type: {self.element_type}\n' +
            f'size: {self.size()}\n' +
            f'read cursor position: {self.read_cursor.value}\n' + 
            f'write cursor position: {self.write_cursor.value}\n' +
            f'lost item: {self.lost_item.value}\n' +
            f'buffer: {self.data}\n' + 
            f'{self.view_data()}\n'
        )

        return reprstr        
