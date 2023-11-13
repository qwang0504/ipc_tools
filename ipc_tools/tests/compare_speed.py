from multiprocessing import Process, Event
from typing import Callable, Tuple
import numpy as np
from numpy.typing import NDArray
import time
import pandas as pd
import seaborn as sns
from tqdm import tqdm
import matplotlib.pyplot as plt
from ipc_tools import MonitoredQueue, QueueMP, RingBuffer, ZMQ_PushPullArray
from scipy.stats import mode
from timeit import timeit
from queue import Empty, Full

def consumer(
        buf: MonitoredQueue, 
        processing_fun: Callable, 
        stop: Event, 
        block: bool, 
        timeout: float
    ):

    while not stop.is_set():
        try:
            array = buf.get(block=block, timeout=timeout) # this can return None
            processing_fun(array)
        except Empty:
            pass

def producer(
        buf: MonitoredQueue, 
        stop: Event,
        block: bool, 
        timeout: float
    ):

    while not stop.is_set():
        try:
            buf.put(BIGARRAY, block=block, timeout=timeout)
        except Full:
            pass
    
def do_nothing(array: NDArray) -> None:
    pass

def average(array: NDArray) -> None:
    mu = np.mean(array)

def long_computation_mt(array: NDArray) -> None:
    # long multithreaded computation  
    U,S,V = np.linalg.svd(array[0:256,0:256])

def long_computation_st(array: NDArray) -> None:
    # long single-threaded computation
    mode(array[0:256,0:256], keepdims=False)

def run(
        buffer: MonitoredQueue, 
        processing_fun: Callable = do_nothing, 
        num_prod: int = 1, 
        num_cons: int = 1, 
        t_measurement: float = 2.0,
        block_put: bool = False,
        timeout_put: float = 2.0,
        block_get: bool = False,
        timeout_get: float = 2.0
    ) -> Tuple[float, float]:
   
    # shared event to stop producers and consumers
    stop = Event()

    # spin up processes
    processes = []
    for i in range(num_cons):
        p = Process(target=consumer,args=(buffer, processing_fun, stop, block_get, timeout_get))
        p.start()
        processes.append(p)

    for i in range(num_prod):
        p = Process(target=producer,args=(buffer, stop, block_put, timeout_put))
        p.start()
        processes.append(p)
        
    # measure some time 
    time.sleep(t_measurement)

    # stop 
    stop.set()

    for p in processes:
        p.join()

    return buffer.get_average_freq()

if __name__ == '__main__':

    nprod = 1 # zmq direct push/pull and array queue support only one producer
    reps = 1
    timing_data = pd.DataFrame(columns=['pfun','shm','ncons','fps_in','fps_out', 'frame_sz'])

    for SZ in tqdm([(256,256),(512,512),(1024,1024),(2048,2048)], desc="frame size", position = 0):

        BIGARRAY = np.random.randint(0, 255, SZ, dtype=np.uint8)
        max_size_MB = int(2000*np.prod(SZ)/(1024**2))

        # check execution time of processing functions
        #for pfun in [do_nothing, average, long_computation_st, long_computation_mt]:
        #    print(f'{pfun.__name__} : {timeit(lambda: pfun(BIGARRAY), number=10)} s')

        for ncons in tqdm([1,2,3,4,5,10,25], desc="num consumers", position = 1, leave=False):
            for pfun in tqdm([do_nothing, average, long_computation_st, long_computation_mt], desc="proc function", position = 2, leave=False):
                for rep in tqdm(range(reps), desc="repetitions", position = 3, leave=False):

                    buffers = {
                        'Ring buffer': MonitoredQueue(
                            RingBuffer(
                                num_items = 100, 
                                item_shape = SZ,
                                data_type = np.uint8
                            )),
                        'ZMQ': MonitoredQueue(
                            ZMQ_PushPullArray(
                                item_shape = SZ,
                                data_type = np.uint8,
                                port = 5557
                            )),
                        'Queue': MonitoredQueue(QueueMP())
                    }

                    for name, buf in tqdm(buffers.items(), desc="IPC type", position = 4, leave=False):

                        fps_in, fps_out = run(
                            buffer = buf, 
                            processing_fun = pfun, 
                            t_measurement = 2,
                            block_put = False,
                            timeout_put = 2.0,
                            block_get = True,
                            timeout_get = 2.0, 
                            num_cons = ncons, 
                            num_prod = nprod
                        )

                        row = pd.DataFrame.from_dict({
                            'pfun': [pfun.__name__], 
                            'shm': [name],
                            'ncons': [ncons],
                            'fps_in': [fps_in],
                            'fps_out': [fps_out],
                            'frame_sz': [int(np.prod(SZ))]
                        })
                        timing_data = pd.concat([timing_data, row], ignore_index=True)

    g = sns.FacetGrid(
        timing_data, 
        col="pfun", 
        row="frame_sz", 
        hue="shm", 
        sharey=False, 
        height = 3, 
        aspect = 1.5
    )  
    g.map_dataframe(sns.lineplot, x="ncons", y="fps_out")
    g.add_legend()
    plt.show()
    
    g = sns.FacetGrid(
        timing_data, 
        col="pfun", 
        row="frame_sz", 
        hue="shm", 
        sharey=False, 
        height = 3, 
        aspect = 1.5
    )   
    g.map_dataframe(sns.lineplot, x="ncons", y="fps_in")
    g.add_legend()
    plt.show()
