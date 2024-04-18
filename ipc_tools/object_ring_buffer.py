from .queue_like import QueueLike
from typing import Optional, Any, Callable
from numpy.typing import NDArray, ArrayLike, DTypeLike
from ipc_tools import RingBuffer
import numpy as np
from multiprocessing_logger import Logger
 
class ObjectRingBuffer(QueueLike):

    def __init__(
            self, 
            serialize: Callable[[Any], NDArray], 
            deserialize: Callable[[NDArray], Any],
            data_type: DTypeLike,
            item_shape: ArrayLike = (1,),
            num_items: int = 100,
            t_refresh: float = 1e-6,
            copy: bool = False,
            name: str = '', 
            logger: Optional[Logger] = None,
        ) -> None:

        super().__init__()

        self.num_items = num_items
        self.t_refresh = t_refresh
        self.item_shape = item_shape
        self.data_type = np.dtype(data_type)
        self.copy = copy
        self.name = name

        # create default RingBuffer
        self.queue = RingBuffer(
            num_items = num_items,
            item_shape = item_shape,
            data_type = data_type,
            t_refresh = t_refresh,
            copy = copy,
            logger = logger,
            name = name
        )

        self.serialize = serialize
        self.deserialize = deserialize

    def qsize(self) -> int:
        return self.queue.qsize()

    def empty(self) -> bool:
        return self.queue.empty()
    
    def full(self) -> bool:
        return self.queue.full()

    def put(self, obj: Any, block: Optional[bool] = True, timeout: Optional[float] = None) -> None:
        array = self.serialize(obj) 
        self.queue.put(array, block, timeout)
    
    def get(self, block: Optional[bool] = True, timeout: Optional[float] = None) -> Any:
        array = self.queue.get(block, timeout)
        obj = self.deserialize(array)
        return obj

    def close(self) -> None:
        self.queue.close()

    def join_thread(self) -> None:
        self.queue.join_thread()

    def cancel_join_thread(self) -> None:
        self.queue.cancel_join_thread()

    @property
    def num_lost_item(self):
        return self.queue.num_lost_item
