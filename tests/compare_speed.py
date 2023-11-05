from multiprocessing import Process, Event
from typing import Callable
import numpy as np
from numpy.typing import NDArray
import time

SZ = (2048,2048) # use a size of (1024,1024) to measure throughput in MB/s
BIGARRAY = np.random.randint(0, 255, SZ, dtype=np.uint8)

from ring_buffer import  OverflowRingBuffer_Locked, MultiRingBuffer_Locked
from monitored_ipc import MonitoredIPC, MonitoredQueue, MonitoredRingBuffer, MonitoredZMQ_PushPull, MonitoredArrayQueue

def consumer(buf: MonitoredIPC, processing_fun: Callable, stop: Event, timeout: float):
    buf.initialize_receiver()
    while not stop.is_set():
        array = buf.get(timeout=timeout)
        processing_fun(array)

def producer(buf: MonitoredIPC, stop: Event):
    buf.initialize_sender()
    while not stop.is_set():
        buf.put(BIGARRAY)
        time.sleep(0.0000001) # this is necessary for ring buffer to perform correctly

def do_nothing(array: NDArray) -> None:
    pass

def average(array: NDArray) -> None:
    mu = np.mean(array)

def long_computation(array: NDArray) -> None:
    U,S,V = np.linalg.svd(array[0:128,0:128])

def run(
        buffer: MonitoredIPC, 
        processing_fun: Callable = do_nothing, 
        num_prod: int = 1, 
        num_cons: int = 1, 
        t_measurement: float = 2.0,
        timeout: float = 2.0
    ):
   
    # shared event to stop producers and consumers
    stop = Event()

    # spin up processes
    processes = []
    for i in range(num_cons):
        p = Process(target=consumer,args=(buffer, processing_fun, stop, timeout))
        p.start()
        processes.append(p)

    for i in range(num_prod):
        p = Process(target=producer,args=(buffer, stop))
        p.start()
        processes.append(p)
        
    # measure some time 
    time.sleep(t_measurement)

    # stop 
    stop.set()
    for p in processes:
        p.terminate()

if __name__ == '__main__':

    max_size_MB = int(100*np.prod(SZ)/(1024**2))

    for nprod in range(1,3):
        for ncons in range(1,5):
            
            buffers = {
                'Ring buffer':  MonitoredRingBuffer(
                        num_items = 100, 
                        item_shape = SZ,
                        data_type = np.uint8
                    ),
                'Array Queue':  MonitoredArrayQueue(max_mbytes=max_size_MB),
                'ZMQ':  MonitoredZMQ_PushPull(
                        item_shape = SZ,
                        data_type = np.uint8,
                        port = 5557
                    ),
                'Queue': MonitoredQueue()
            }

            for name, buf in buffers.items():

                print(f'{name},  {ncons} consumers, {nprod} producers ' + 60 * '-' + '\n')
                run(buffer = buf, num_cons = ncons, num_prod=nprod)
                print('\n\n')
