from abc import ABC, abstractmethod
from typing import Optional, Any
from multiprocessing.queues import Queue
from queue import Empty
from multiprocessing import get_context

class QueueLike(ABC):
    '''
    Multiprocessing Queue-like interface for various IPC methods
    '''

    # Queue-like methods --------------------------------------------------------------

    @abstractmethod
    def qsize(self) -> int:
        pass

    @abstractmethod
    def empty(self) -> bool:
        pass
    
    @abstractmethod 
    def full(self) -> bool:
        pass

    @abstractmethod
    def put(self, obj: Any, block: Optional[bool] = True, timeout: Optional[float] = None) -> None:
        '''
        Raises queue.Full exception if block is True and timeout 
        '''
        pass
    
    def put_nowait(self, obj: Any) -> None:
        self.put(obj, False)

    @abstractmethod
    def get(self, block: Optional[bool] = True, timeout: Optional[float] = None) -> Any:
        '''
        Raises queue.Empty exception if block is True and timeout 
        '''
        pass

    def get_nowait(self) -> Any:
        return self.get(False)
    
    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def clear(self) -> None:
        pass

    def join_thread(self) -> None:
        pass

    def cancel_join_thread(self) -> None:
        pass

    @abstractmethod
    def get_num_items(self) -> Optional[int]:
        pass

class QueueMP(Queue, QueueLike):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs, ctx = get_context())

    def clear(self) -> None:
        try:
            while True:
                self.get_nowait()
        except Empty:
            pass

    def get_num_items(self) -> Optional[int]:
        return None