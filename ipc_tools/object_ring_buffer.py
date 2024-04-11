from .queue_like import QueueLike
from typing import Optional, Any, Callable
from numpy.typing import NDArray, ArrayLike, DTypeLike
from ipc_tools import RingBuffer
import numpy as np

# serialize/deserialize must produce/take a numpy array COMPATIBLE with the ring buffer DTYPE
# serialize: Callable[[Any], NDArray] can take a list of objects as input and convert to a single NDArray, that can be nice for metadata

# IDEA: have the ObjectRingBuffer create and update RingBuffer on the fly based on serialize output 
 
class ObjectRingBuffer(QueueLike):

    def __init__(
            self, 
            serialize: Callable[[Any], NDArray], 
            deserialize: Callable[[NDArray], Any],
            data_type: DTypeLike,
            item_shape: ArrayLike = (1,),
            num_items: int = 100,
            t_refresh: float = 1e-6,
            copy: bool = False
        ) -> None:

        super().__init__()

        self.num_items = num_items
        self.t_refresh = t_refresh
        self.item_shape = item_shape
        self.data_type = np.dtype(data_type)
        self.copy = copy

        # create default RingBuffer
        self.queue = RingBuffer(
            num_items = num_items,
            item_shape = item_shape,
            data_type = data_type,
            t_refresh = t_refresh,
            copy = copy
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
        self.queue.num_lost_item
