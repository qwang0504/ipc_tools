from multiprocessing import Process, Event
import numpy as np
import time
import cv2

from ipc_tools import  OverflowRingBuffer_Locked, MultiRingBuffer_Locked
from ipc_tools import MonitoredIPC, MonitoredQueue, MonitoredRingBuffer, MonitoredZMQ_PushPull, MonitoredArrayQueue

SZ = (2048,2048) # use a size of (1024,1024) to measure throughput in MB/s
BIGARRAY = np.random.randint(0, 255, SZ, dtype=np.uint8)

def consumer_cv(buf: MonitoredIPC, stop: Event, sleep_time: float):
    buf.initialize_receiver()
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

def producer_random(buf: MonitoredIPC, stop: Event, sleep_time: float):
    while not stop.is_set():
        buf.put(np.random.randint(0, 255, SZ, dtype=np.uint8))

def consumer(buf: MonitoredIPC, stop: Event, sleep_time: float):
    buf.initialize_receiver()
    start = time.time()
    count = 0
    while not stop.is_set():
        array = buf.get(timeout=2)
        time.sleep(sleep_time)
        if array is not None:
            count += 1
    elapsed = time.time() - start
    print((elapsed,count/elapsed))

def producer(buf: MonitoredIPC, stop: Event, sleep_time: float):
    buf.initialize_sender()
    while not stop.is_set():
        buf.put(BIGARRAY)
        time.sleep(sleep_time)

def monitor(buf: MonitoredIPC, stop: Event, sleep_time: float):
    buf.initialize_sender()
    while not stop.is_set():
        print(buf.size())
        time.sleep(sleep_time)

def test_00():
    '''
    - 1 producer 
    - 1 consumer
    - producer and consumer ~ same speed
    '''

    buffer = MonitoredRingBuffer(
        num_items = 100, 
        item_shape = SZ,
        data_type = np.uint8
    )

    stop = Event()

    p0 = Process(target=producer,args=(buffer,stop,0.001))
    p1 = Process(target=consumer,args=(buffer,stop,0.001))
    #p2 = Process(target=monitor,args=(buffer,stop,0.1))

    p0.start()
    p1.start()
    #p2.start()

    time.sleep(4)
    stop.set()

    p0.join()
    p1.join()
    #p2.join()

def test_00_q():
    '''
    - 1 producer 
    - 1 consumer
    - producer and consumer ~ same speed
    - Uses Queue
    '''

    buffer = MonitoredQueue()

    stop = Event()

    p0 = Process(target=producer,args=(buffer,stop,0.001))
    p1 = Process(target=consumer,args=(buffer,stop,0.001))
    #p2 = Process(target=monitor,args=(buffer,stop,0.1))

    p0.start()
    p1.start()
    #p2.start()

    time.sleep(4)
    stop.set()
    time.sleep(4)

    p0.terminate()
    p1.terminate()
    #p2.join()

def test_01():
    '''
    - 1 producer 
    - 1 consumer
    - producer faster than consumer
    '''

    buffer = MonitoredRingBuffer(
        num_items = 100, 
        item_shape = SZ,
        data_type = np.uint8
    )

    stop = Event()

    p0 = Process(target=producer,args=(buffer,stop,0.001))
    p1 = Process(target=consumer,args=(buffer,stop,0.002))
    #p2 = Process(target=monitor,args=(buffer,stop,0.1))

    p0.start()
    p1.start()
    #p2.start()

    time.sleep(4)
    stop.set()

    p0.join()
    p1.join()
    #p2.join()

def test_02():
    '''
    - 1 producer 
    - 1 consumer
    - consumer faster than producer
    '''

    buffer = MonitoredRingBuffer(
        num_items = 100, 
        item_shape = SZ,
        data_type = np.uint8
    )

    stop = Event()

    p0 = Process(target=producer,args=(buffer,stop,0.002))
    p1 = Process(target=consumer,args=(buffer,stop,0.001))
    #p2 = Process(target=monitor,args=(buffer,stop,0.1))

    p0.start()
    p1.start()
    #p2.start()

    time.sleep(4)
    stop.set()

    p0.join()
    p1.join()
    #p2.join()


def test_02bis():
    '''
    - 1 producer 
    - 1 consumer
    - AFAP
    '''

    buffer = MonitoredRingBuffer(
        num_items = 100, 
        item_shape = SZ,
        data_type = np.uint8
    )

    stop = Event()

    p0 = Process(target=producer,args=(buffer,stop,0.000000001))
    p1 = Process(target=consumer,args=(buffer,stop,0.000000001))
    #p2 = Process(target=monitor,args=(buffer,stop,0.1))

    p0.start()
    p1.start()
    #p2.start()

    time.sleep(4)
    stop.set()

    p0.join()
    p1.join()
    #p2.join()

def test_02bis_q():
    '''
    - 1 producer 
    - 1 consumer
    - AFAP
    - Uses Queue
    '''

    buffer = MonitoredQueue()

    stop = Event()

    p0 = Process(target=producer,args=(buffer,stop,0.000000001))
    p1 = Process(target=consumer,args=(buffer,stop,0.000000001))
    #p2 = Process(target=monitor,args=(buffer,stop,0.1))

    p0.start()
    p1.start()
    #p2.start()

    time.sleep(4)
    stop.set()
    time.sleep(4)

    p0.terminate()
    p1.terminate()
    #p2.join()

def test_02bis_aq():
    '''
    - 1 producer 
    - 1 consumer
    - AFAP
    - Uses Array Queue
    '''

    buffer = MonitoredArrayQueue(max_mbytes=int(100*np.prod(SZ)/(1024**2)))

    stop = Event()

    p0 = Process(target=producer,args=(buffer,stop,0.000000001))
    p1 = Process(target=consumer,args=(buffer,stop,0.000000001))
    #p2 = Process(target=monitor,args=(buffer,stop,0.1))

    p0.start()
    p1.start()
    #p2.start()

    time.sleep(4)
    stop.set()
    time.sleep(4)

    p0.terminate()
    p1.terminate()
    #p2.join()

def test_02bis_z():
    '''
    - 1 producer 
    - 1 consumer
    - AFAP
    - Uses ZMQ sockets
    '''

    buffer = MonitoredZMQ_PushPull(
        item_shape = SZ,
        data_type = np.uint8,
        port = 5556
    )

    stop = Event()

    p0 = Process(target=producer,args=(buffer,stop,0.000000001))
    p1 = Process(target=consumer,args=(buffer,stop,0.000000001))
    #p2 = Process(target=monitor,args=(buffer,stop,0.1))

    p0.start()
    p1.start()
    #p2.start()

    time.sleep(4)
    stop.set()
    time.sleep(4)

    p0.terminate()
    p1.terminate()
    #p2.join()

def test_03():
    '''
    - 2 producer 
    - 1 consumer
    '''

    buffer = MonitoredRingBuffer(
        num_items = 100, 
        item_shape = SZ,
        data_type = np.uint8
    )

    stop = Event()

    p0 = Process(target=producer,args=(buffer,stop,0.001))
    p1 = Process(target=producer,args=(buffer,stop,0.001))
    p2 = Process(target=consumer,args=(buffer,stop,0.001))
    #p3 = Process(target=monitor,args=(buffer,stop,0.1))

    p0.start()
    p1.start()
    p2.start()
    #p3.start()

    time.sleep(4)
    stop.set()

    p0.join()
    p1.join()
    p2.join()
    #p3.join()

def test_04():
    '''
    - 1 producer 
    - 2 consumer
    '''

    buffer = MonitoredRingBuffer(
        num_items = 100, 
        item_shape = SZ,
        data_type = np.uint8
    )

    stop = Event()

    p0 = Process(target=producer,args=(buffer,stop,0.001))
    p1 = Process(target=consumer,args=(buffer,stop,0.001))
    p2 = Process(target=consumer,args=(buffer,stop,0.001))
    #p3 = Process(target=monitor,args=(buffer,stop,0.1))

    p0.start()
    p1.start()
    p2.start()
    #p3.start()

    time.sleep(4)
    stop.set()

    p0.join()
    p1.join()
    p2.join()
    #p3.join()

def test_05():
    '''
    - 1 producer 
    - 1 consumer with cv2 display
    - producer and consumer ~ same speed
    '''

    buffer = MonitoredRingBuffer(
        num_items = 100, 
        item_shape = SZ,
        data_type = np.uint8
    )

    stop = Event()

    p0 = Process(target=producer_random,args=(buffer,stop,0.001))
    p1 = Process(target=consumer_cv,args=(buffer,stop,0.001))
    #p2 = Process(target=monitor,args=(buffer,stop,0.1))

    p0.start()
    p1.start()
    #p2.start()

    time.sleep(4)
    stop.set()

    p0.join()
    p1.join()
    #p2.join()

def test_overflow():
    '''
    overflow
    '''

    buffer = OverflowRingBuffer_Locked(
        num_items = 5, 
        item_shape = (1,),
        data_type = np.uint8
    )

    buffer.put(np.array(1, dtype=np.uint8))
    buffer.put(np.array(2, dtype=np.uint8))
    buffer.put(np.array(3, dtype=np.uint8))
    buffer.put(np.array(4, dtype=np.uint8))
    buffer.put(np.array(5, dtype=np.uint8))
    buffer.put(np.array(6, dtype=np.uint8))

def test_types():
    '''
    types
    '''

    buffer = OverflowRingBuffer_Locked(
        num_items = 5, 
        item_shape = (10,),
        data_type = np.float32
    )

    buffer.put(np.ones(shape=(10,), dtype=np.float32))
    buffer.put(np.ones(shape=(10,), dtype=np.float64))
    buffer.put(np.ones(shape=(10,), dtype=np.int))

    buffer = OverflowRingBuffer_Locked(
        num_items = 5, 
        item_shape = (10,),
        data_type = np.float32
    )

    buffer = OverflowRingBuffer_Locked(
        num_items = 5, 
        item_shape = (10,),
        data_type = np.uint16
    )

    buffer = OverflowRingBuffer_Locked(
        num_items = 5, 
        item_shape = (10,),
        data_type = np.float64
    )

    
def test_shape():
    '''
    shape
    '''

    buffer = OverflowRingBuffer_Locked(
        num_items = 5, 
        item_shape = 10,
        data_type = np.float32
    )

    buffer.put(np.array([1,2,3,4,5,6,7,8,9,10]))
    buffer.put([11,12,13,14,15,16,17,18,19,20])
    buffer.put(np.arange(10))

def test_multiringbuffer():

    multibuffer = MultiRingBuffer_Locked(
        num_items=5,
        item_shape=[(1,),(1,),(256,256)],
        data_type=[np.float32, np.uint, np.uint8]
    )

    multibuffer.put([time.time(), 0, np.random.randint(0,255,(256,256),np.uint8)])
    multibuffer.put([time.time(), 1, np.random.randint(0,255,(256,256),np.uint8)])
    multibuffer.put([time.time(), 2, np.random.randint(0,255,(256,256),np.uint8)])
    multibuffer.put([time.time(), 3, np.random.randint(0,255,(256,256),np.uint8)])

    print(multibuffer)

    timestamp, index, frame = multibuffer.get()
    timestamp, index, frame = multibuffer.get()
    timestamp, index, frame = multibuffer.get()
    timestamp, index, frame = multibuffer.get()
    timestamp, index, frame = multibuffer.get(timeout=2)

    print(multibuffer)

if __name__ == '__main__':
    test_fun = [test_02bis, test_02bis_aq, test_02bis_q, test_02bis_z]
    for f in test_fun:
        print(f.__doc__)
        f()