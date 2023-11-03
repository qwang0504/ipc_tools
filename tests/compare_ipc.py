import numpy as np
import zmq
from ring_buffer import RingBuffer, OverflowRingBuffer_Locked
from multiprocessing import Process, Event, Queue, Value
import multiprocessing as mp
import time
from typing import Callable
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm

SHAPE = (2048, 2048)
BIG_ARRAY = np.random.randint(0,255,SHAPE, dtype='B')
NLOOP = 200
REPEATS = 20

def consumer_ringbuffer(
        buffer: RingBuffer, 
        nloop: int, 
        processing_fun: Callable,
        stop_time: Value
    ) -> None:

    # start timing
    start_time = time.time_ns()

    # loop
    for i in range(nloop):
        # get data
        array = buffer.get(blocking=True)
        # process
        processing_fun(array)

    # stop timing
    stop_time.value = 1e-9*(time.time_ns() - start_time)

def consumer_zmq(
        data_available: Event, 
        nloop: int,
        processing_fun: Callable,
        stop_time: Value
    ) -> None:

    # configure zmq
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://localhost:5555")

    # start timing
    start_time = time.time_ns()

    # loop
    for i in range(nloop):
        #get data
        data_available.wait()
        data = socket.recv()
        array = np.frombuffer(data, dtype='B')
        
        # process
        processing_fun(array)

    # stop timing
    stop_time.value = 1e-9*(time.time_ns() - start_time)

def consumer_queue(
        data_available: Event, 
        queue: Queue, 
        nloop: int,
        processing_fun: Callable,
        stop_time: Value
    ) -> None:

    # start timing
    start_time = time.time_ns()

    # loop
    for i in range(nloop):
        # get data
        data_available.wait()
        data = queue.get()
        array = np.frombuffer(data, dtype='B')

        # processs
        processing_fun(array)

    # stop timing
    stop_time.value = 1e-9*(time.time_ns() - start_time)

def test_ring_buffer(processing_fun: Callable) -> float:
    ## shared ring buffer -------------------------------------
    buffer = OverflowRingBuffer_Locked(
        num_items = NLOOP, 
        item_shape = SHAPE,
        data_type= np.uint8
    )
    
    stop_time = Value('d',0) 
    proc = Process(
        target=consumer_ringbuffer, 
        args=(buffer, NLOOP, processing_fun, stop_time)
    )
    proc.start()

    # loop
    for i in range(NLOOP):
        buffer.put(BIG_ARRAY)

    # done
    proc.join()
    return stop_time.value

def test_zmq(processing_fun: Callable) -> float:
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://*:5555")
    data_available = Event()
    stop_time = Value('d',0) 

    proc = Process(
        target=consumer_zmq, 
        args=(data_available, NLOOP, processing_fun, stop_time)
    )
    proc.start()

    # loop
    for i in range(NLOOP):
        socket.send(BIG_ARRAY.reshape((BIG_ARRAY.nbytes,)))
        data_available.set()

    # done
    proc.join()
    return stop_time.value

def test_queues(processing_fun: Callable) -> float:
    queue = Queue()
    data_available = Event()
    stop_time = Value('d',0) 
    proc = Process(
        target=consumer_queue, 
        args=(data_available, queue, NLOOP, processing_fun, stop_time)
    )
    proc.start()
    
    # loop
    for i in range(NLOOP):
        queue.put(BIG_ARRAY.reshape((BIG_ARRAY.nbytes,)))
        data_available.set()

    # done
    proc.join()
    return stop_time.value

def do_nothing(array):
    pass

def average(array):
    array_2D = array.reshape(SHAPE)
    mu = np.mean(array_2D)

def long_computation(array):
    array_2D = array.reshape(SHAPE)
    U,S,V = np.linalg.svd(array_2D[0:256,0:256])
    
if __name__ == '__main__':
    #mp.set_start_method('spawn')
    timing_data = pd.DataFrame(columns=['pfun','shm','timing'])
    for processing_fun_name, processing_fun in zip(['pass','avg','svd'],[do_nothing, average, long_computation]):
        print(processing_fun_name)
        for rep in tqdm(range(REPEATS)):
            row_0 = pd.DataFrame.from_dict({
                'pfun': [processing_fun_name], 
                'shm': ['rb'] ,
                'timing': [test_ring_buffer(processing_fun)]
            })
            row_1 = pd.DataFrame.from_dict({
                'pfun': [processing_fun_name], 
                'shm': ['zmq'],
                'timing': [test_zmq(processing_fun)]
            })
            row_2 = pd.DataFrame.from_dict({
                'pfun': [processing_fun_name], 
                'shm': ['queue'] ,
                'timing': [test_queues(processing_fun)]
            })
            timing_data = pd.concat([timing_data, row_0, row_1, row_2], ignore_index=True)

    plt.figure()
    ax = sns.catplot(timing_data, x="shm", y="timing", col="pfun", kind="bar")
    plt.show()