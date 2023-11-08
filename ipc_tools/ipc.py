from abc import ABC, abstractmethod
from typing import Optional, Any

class QueueLikeIPC(ABC):
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

    def join_thread(self) -> None:
        pass

    def cancel_join_thread(self) -> None:
        pass

    # extra methods  --------------------------------------------------------------

    @abstractmethod
    def initialize_receiver(self) -> None:
        '''
        function to execute in the receiver process
        '''
        pass

    @abstractmethod
    def initialize_sender(self) -> None:
        '''
        function to execute in the sender process
        '''
        pass