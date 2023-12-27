from ipc_tools import PriorityQueue, MonitoredQueue
import numpy as np
from multiprocessing import Process, Event
import cv2
import time
from queue import Empty

# test basic functionality

SZ = (4,4)
ARRAY_0 = np.random.randint(0, 255, SZ, dtype=np.int32)
ARRAY_1 = np.random.randint(0, 255, SZ, dtype=np.int32)
ARRAY_2 = np.random.randint(0, 255, SZ, dtype=np.int32)

Q = PriorityQueue(        
        num_items = 100, 
        item_shape = SZ,
        data_type = np.int32
    )

Q.put((1, ARRAY_0))
Q.put((10, ARRAY_1))
Q.put((2, ARRAY_2))

print(Q)

assert(np.allclose(Q.get(), ARRAY_1))
assert(np.allclose(Q.get(), ARRAY_2))
assert(np.allclose(Q.get(), ARRAY_0))

# test multiprocessing

def consumer_cv(buf: MonitoredQueue, stop: Event):
    cv2.namedWindow('display')
    start = time.time()
    count = 0
    while not stop.is_set():
        array = buf.get(timeout=2)
        if array is not None:
            count += 1
            cv2.imshow('display',array)
            cv2.waitKey(1)
    elapsed = time.time() - start
    cv2.destroyAllWindows()
    print((elapsed,count/elapsed))

def producer_random(buf: MonitoredQueue, stop: Event):
    priority = 0
    while not stop.is_set():
        priority += 1 
        buf.put((priority, np.random.randint(0, 255, SZ, dtype=np.uint8)))

SZ = (2048, 2048)
BIGARRAY = np.random.randint(0, 255, SZ, dtype=np.uint8)

Q = PriorityQueue(        
        num_items = 100, 
        item_shape = SZ,
        data_type = np.uint8
    )

buffer = MonitoredQueue(Q)

stop = Event()

p0 = Process(target=producer_random,args=(buffer,stop))
p1 = Process(target=consumer_cv,args=(buffer,stop))

p0.start()
p1.start()

time.sleep(4)
stop.set()
time.sleep(4)

p0.terminate()
p1.terminate()

buffer.get_average_freq()

# as fast as possible 

def consumer_fast(buf: MonitoredQueue, stop: Event):
    start = time.time()
    count = 0
    while not stop.is_set():
        try:
            array = buf.get(block=True, timeout=2)
            if array is not None:
                count += 1
        except Empty:
            pass
    elapsed = time.time() - start
    print((elapsed,count/elapsed))

def producer_fast(buf: MonitoredQueue, stop: Event):
    priority = 0
    while not stop.is_set():
        priority += 1 
        buf.put((priority, BIGARRAY))

Q = PriorityQueue(        
        num_items = 10, 
        item_shape = SZ,
        data_type = np.uint8
    )

buffer = MonitoredQueue(Q)

stop = Event()

p0 = Process(target=producer_fast,args=(buffer,stop))
p1 = Process(target=consumer_fast,args=(buffer,stop))

p0.start()
p1.start()

time.sleep(4)
stop.set()
time.sleep(4)

p0.terminate()
p1.terminate()

print(f'Freq in, freq out: {buffer.get_average_freq()}') 
print(f'Num item lost: {buffer.queue.num_lost_item.value}')
# I have very variable performance on different runs
# what's going on ? Maybe argmin/argmax worst case.
# I should profile  
# Much faster and more reliable with small queue size (~10)

# when blocking and empty, get_noblock raises Empty immediately ?
# should also be true for ring buffer ? Unless specifically try except
# in consume. Maybe get with timeout should try except