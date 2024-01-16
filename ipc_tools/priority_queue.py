from multiprocessing import RawArray, RawValue, RLock
from ipc_tools import QueueLike
from typing import Optional, Tuple
import numpy as np
from numpy.typing import NDArray, ArrayLike, DTypeLike
import time
from queue import Empty
from ipc_tools import SharedMinMaxHeapTuple

PRIORITY_EMPTY = 0
# TODO use a heap to store the priorities

class PriorityQueue(QueueLike):
    '''
    put takes a tuple as argument: (priority, argument)
    priority must be strictly positive, higher number means higher priority
    '''

    def __init__(
            self,
            num_items: int,
            item_shape: ArrayLike,
            data_type: DTypeLike,
            t_refresh: float = 0.000001,
            copy: bool = False
        ):
        
        self.item_shape = np.asarray(item_shape)
        self.element_type = np.dtype(data_type)
        self.t_refresh = t_refresh
        self.copy = copy

        # account for empty slot
        self.num_items = num_items + 1 
        self.item_num_element = int(np.prod(self.item_shape))
        self.element_byte_size = self.element_type.itemsize 
        self.total_size =  self.item_num_element * self.num_items
        
        self.lock = RLock()
        self.priority = RawArray('I', [PRIORITY_EMPTY for i in range(self.num_items)])
        self.element_location = RawArray('I', range(0, self.element_byte_size*self.total_size, self.element_byte_size*self.item_num_element))
        self.num_lost_item = RawValue('I',0)
        self.num_elem = RawValue('I',0)
        self.data = RawArray('B', self.total_size*self.element_byte_size) 
    
    def get_lowest_priority(self) -> Tuple[int, int]:
        '''return index of lowest priority item'''
        lowest_priority_index = np.argmin(self.priority)
        return (lowest_priority_index, self.element_location[lowest_priority_index])

    def get_highest_priority(self) -> Tuple[int, int]:
        '''return index of highest priority item'''
        highest_priority_index = np.argmax(self.priority)
        return (highest_priority_index, self.element_location[highest_priority_index])
        
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

        with self.lock:

            if self.empty():
                raise Empty
            
            (element_index, element_location) = self.get_highest_priority()

            if self.copy:
                element = np.frombuffer(
                    self.data, 
                    dtype = self.element_type, 
                    count = self.item_num_element,
                    offset = element_location # offset should be in bytes
                ).copy()
            else:
                element = np.frombuffer(
                    self.data, 
                    dtype = self.element_type, 
                    count = self.item_num_element,
                    offset = element_location # offset should be in bytes
                )
            self.priority[element_index] = PRIORITY_EMPTY
            self.num_elem.value -= 1

        return element.reshape(self.item_shape)
    
    def put(self, data: Tuple, block: Optional[bool] = True, timeout: Optional[float] = None) -> None:
        '''
        Return data at the current write location.
        block and timeout are there for compatibility with the Queue interface, but 
        are ignored since the ring buffer overflows by design.  
        '''

        priority, element = data

        # convert to numpy array
        arr_element = np.asarray(element, dtype = self.element_type)

        with self.lock:

            (element_index, element_location) = self.get_lowest_priority()

            buffer = np.frombuffer(
                self.data, 
                dtype = self.element_type, 
                count = self.item_num_element,
                offset = element_location # offset should be in bytes
            )

            # if the buffer is full, overwrite the next block
            if self.full():
                self.num_lost_item.value += 1

            # write flattened array content to buffer
            buffer[:] = arr_element.ravel()

            # update write cursor value
            self.priority[element_index] = priority

            self.num_elem.value += 1

        # this seems to be necessary to give time to consumers to get the lock 
        time.sleep(self.t_refresh)

    def full(self):
        ''' check if buffer is full '''
        if self.num_elem.value == self.num_items-1:
            return True
        else:
            return False
    
    def empty(self):
        ''' check if buffer is empty '''
        if self.num_elem.value == 0:
            return True
        else:
            return False

    def qsize(self):
        ''' Return number of items currently stored in the buffer '''
        return self.num_elem.value
    
    def close(self):
        pass

    def clear(self):
        '''clear the buffer'''
        self.priority = RawArray('I', [PRIORITY_EMPTY for i in range(self.num_items)])

    def view_data(self):

        stored_data = np.array([])
        for element_index, element_priority  in enumerate(self.priority):
            if element_priority != PRIORITY_EMPTY:
                location = self.element_location[element_index]
                stored_element = np.frombuffer(                
                    self.data, 
                    dtype = self.element_type, 
                    count = self.item_num_element,
                    offset = location # offset should be in bytes
                )
                stored_data = np.hstack((stored_data, stored_element))

        return stored_data
    
    def __str__(self):
        
        reprstr = (
            f'capacity: {self.num_items}\n' +
            f'item shape: {self.item_shape}\n' +
            f'data type: {self.element_type}\n' +
            f'size: {self.qsize()}\n' +
            f'priority, location: {[(p,l) for (p,l) in zip(self.priority, self.element_location) if p != PRIORITY_EMPTY]}\n' + 
            f'lost item: {self.num_lost_item.value}\n' +
            f'buffer: {self.data}\n' + 
            f'{self.view_data()}\n'
        )

        return reprstr
        
class PriorityQueueHeap(QueueLike):

    def __init__(
            self,
            num_items: int,
            item_shape: ArrayLike,
            data_type: DTypeLike,
            t_refresh: float = 0.000001,
            copy: bool = False
        ):
        
        self.item_shape = np.asarray(item_shape)
        self.element_type = np.dtype(data_type)
        self.t_refresh = t_refresh
        self.copy = copy

        # account for empty slot
        self.num_items = num_items + 1 
        self.item_num_element = int(np.prod(self.item_shape))
        self.element_byte_size = self.element_type.itemsize 
        self.total_size =  self.item_num_element * self.num_items
        
        self.lock = RLock()
        self.priority = SharedMinMaxHeapTuple(
            self.num_items, 
            tuplen=2, 
            sortkey=0
        )
        locations = range(0, self.element_byte_size*self.total_size, self.element_byte_size*self.item_num_element)
        initial_priorities = [PRIORITY_EMPTY for i in locations]
        for tup in zip(initial_priorities, locations):
            self.priority.push(tup)

        self.num_lost_item = RawValue('I',0)
        self.num_elem = RawValue('I',0)
        self.data = RawArray('B', self.total_size*self.element_byte_size) 
    
    def get_lowest_priority(self) -> int:
        '''return index of lowest priority item'''
        _, location = self.priority.pop_min()
        return location

    def get_highest_priority(self) -> int:
        '''return index of highest priority item'''
        _, location = self.priority.pop_max()
        return location
        
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

        with self.lock:

            if self.empty():
                raise Empty
            
            element_location = self.get_highest_priority()

            if self.copy:
                element = np.frombuffer(
                    self.data, 
                    dtype = self.element_type, 
                    count = self.item_num_element,
                    offset = int(element_location) # offset should be in bytes
                ).copy()
            else:
                element = np.frombuffer(
                    self.data, 
                    dtype = self.element_type, 
                    count = self.item_num_element,
                    offset = int(element_location) # offset should be in bytes
                )
            self.priority.push((PRIORITY_EMPTY, element_location))
            
            self.num_elem.value -= 1

        return element.reshape(self.item_shape)
    
    def put(self, data: Tuple, block: Optional[bool] = True, timeout: Optional[float] = None) -> None:
        '''
        Return data at the current write location.
        block and timeout are there for compatibility with the Queue interface, but 
        are ignored since the ring buffer overflows by design.  
        '''

        priority, element = data

        # convert to numpy array
        arr_element = np.asarray(element, dtype = self.element_type)

        with self.lock:

            element_location = self.get_lowest_priority()

            buffer = np.frombuffer(
                self.data, 
                dtype = self.element_type, 
                count = self.item_num_element,
                offset = int(element_location) # offset should be in bytes
            )

            # if the buffer is full, overwrite the next block
            if self.full():
                self.num_lost_item.value += 1

            # write flattened array content to buffer
            buffer[:] = arr_element.ravel()

            # update write cursor value
            self.priority.push((priority, element_location))

            self.num_elem.value += 1

        # this seems to be necessary to give time to consumers to get the lock 
        time.sleep(self.t_refresh)

    def full(self):
        ''' check if buffer is full '''
        if self.num_elem.value == self.num_items-1:
            return True
        else:
            return False
    
    def empty(self):
        ''' check if buffer is empty '''
        if self.num_elem.value == 0:
            return True
        else:
            return False

    def qsize(self):
        ''' Return number of items currently stored in the buffer '''
        return self.num_elem.value
    
    def close(self):
        pass

    def clear(self):
        '''clear the buffer'''
        self.priority.clear()
        locations = range(0, self.element_byte_size*self.total_size, self.element_byte_size*self.item_num_element)
        initial_priorities = [PRIORITY_EMPTY for i in locations]
        for tup in zip(initial_priorities, locations):
            self.priority.push(tup)

    def view_data(self):
        stored_data = np.array([])
        priorities = self.priority.get_tuple_elements(0)
        locations = self.priority.get_tuple_elements(1)
        for element_priority, element_location  in zip(priorities, locations):
            if element_priority != PRIORITY_EMPTY:
                stored_element = np.frombuffer(                
                    self.data, 
                    dtype = self.element_type, 
                    count = self.item_num_element,
                    offset = int(element_location) # offset should be in bytes
                )
                stored_data = np.hstack((stored_data, stored_element))

        return stored_data
    
    def __str__(self):

        priorities = self.priority.get_tuple_elements(0)
        locations = self.priority.get_tuple_elements(1)
        reprstr = (
            f'capacity: {self.num_items}\n' +
            f'item shape: {self.item_shape}\n' +
            f'data type: {self.element_type}\n' +
            f'size: {self.qsize()}\n' +
            f'priority, location: {[(p,l) for (p,l) in zip(priorities, locations) if p != PRIORITY_EMPTY]}\n' + 
            f'lost item: {self.num_lost_item.value}\n' +
            f'buffer: {self.data}\n' + 
            f'{self.view_data()}\n'
        )

        return reprstr