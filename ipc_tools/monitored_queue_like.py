from .queue_like import QueueLike
from typing import Optional, Any, Tuple 
from multiprocessing import Value
import time


# TODO: give the queue a logger, log put and get, and plot when the queue is busy
 
class MonitoredQueue(QueueLike):

    def __init__(self, queue: QueueLike) -> None:
        super().__init__()
        self.queue = queue
        
        # store the number of items that were put/retrieved
        self.num_item_in = Value('I',0)
        self.num_item_out = Value('I',0)

        # store the time since first item in/out
        self.time_in = Value('d',0) 
        self.time_out = Value('d',0)
        self.time_in_start = Value('d',0)
        self.time_out_start = Value('d',0)

        # instantaneous frequency
        self.freq_in = Value('d',0)
        self.freq_out = Value('d',0)

    def get_num_items(self) -> int:
        return self.queue.get_num_items()

    def qsize(self) -> int:
        return self.queue.qsize()

    def empty(self) -> bool:
        return self.queue.empty()
    
    def full(self) -> bool:
        return self.queue.full()

    def put(self, obj: Any, block: Optional[bool] = True, timeout: Optional[float] = None) -> None:
        self.queue.put(obj, block, timeout)
        self.account_in()
    
    def get(self, block: Optional[bool] = True, timeout: Optional[float] = None) -> Any:
        res = self.queue.get(block, timeout)
        self.account_out()
        return res

    def close(self) -> None:
        self.queue.close()

    def join_thread(self) -> None:
        self.queue.join_thread()

    def cancel_join_thread(self) -> None:
        self.queue.cancel_join_thread()

    def clear(self) -> None:

        with self.num_item_in.get_lock():
            self.num_item_in.value = 0
            self.time_in.value = 0
            self.time_in_start.value = 0
            self.freq_in.value = 0
        
        with self.num_item_out.get_lock():
            self.num_item_out.value = 0
            self.time_out.value = 0
            self.time_out_start.value = 0
            self.freq_out.value = 0
            
        self.queue.clear()

    def account_in(self) -> None:
        with self.num_item_in.get_lock():
            self.num_item_in.value += 1

            if self.num_item_in.value == 1:
                self.time_in_start.value = time.monotonic()

            previous_time = self.time_in.value
            self.time_in.value = time.monotonic() - self.time_in_start.value
            self.freq_in.value = 1.0/(self.time_in.value - previous_time)

    def account_out(self) -> None:
        with self.num_item_out.get_lock():
            self.num_item_out.value += 1

            if self.num_item_out.value == 1:
                self.time_out_start.value = time.monotonic()

            previous_time = self.time_out.value
            self.time_out.value = time.monotonic() - self.time_out_start.value
            self.freq_out.value = 1.0/(self.time_out.value - previous_time)

    def get_average_freq(self) -> Tuple[float, float]:
        return (
            self.num_item_in.value/self.time_in.value if self.time_in.value > 0 else 0,
            self.num_item_out.value/self.time_out.value if self.time_out.value > 0 else 0
        )