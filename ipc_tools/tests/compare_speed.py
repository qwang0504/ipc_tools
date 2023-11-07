from multiprocessing import Process, Event
from typing import Callable, Tuple
import numpy as np
from numpy.typing import NDArray
import time
import pandas as pd
import seaborn as sns
from tqdm import tqdm
import matplotlib.pyplot as plt
from ipc_tools import MonitoredIPC, MonitoredQueue, MonitoredRingBuffer, MonitoredZMQ_PushPull, MonitoredArrayQueue
from scipy.stats import mode
from timeit import timeit

def consumer(buf: MonitoredIPC, processing_fun: Callable, stop: Event, timeout: float):
    buf.initialize_receiver()
    while not stop.is_set():
        array = buf.get(timeout=timeout)
        processing_fun(array)

def producer(buf: MonitoredIPC, stop: Event):
    buf.initialize_sender()
    while not stop.is_set():
        buf.put(BIGARRAY)
        time.sleep(0.000000001) # this is necessary for ring buffer to perform correctly

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
        buffer: MonitoredIPC, 
        processing_fun: Callable = do_nothing, 
        num_prod: int = 1, 
        num_cons: int = 1, 
        t_measurement: float = 2.0,
        timeout: float = 2.0
    ) -> Tuple[float, float]:
   
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

    return buffer.get_fps()

if __name__ == '__main__':

    nprod = 1 # zmq direct push/pull and array queue support only one producer
    reps = 3
    timing_data = pd.DataFrame(columns=['pfun','shm','ncons','fps_in','fps_out', 'frame_sz'])

    for SZ in [(256,256),(512,512),(1024,1024),(2048,2048)]:

        BIGARRAY = np.random.randint(0, 255, SZ, dtype=np.uint8)
        max_size_MB = int(2000*np.prod(SZ)/(1024**2))

        # check execution time of processing functions
        for pfun in [do_nothing, average, long_computation_st, long_computation_mt]:
            print(f'{pfun.__name__} : {timeit(lambda: pfun(BIGARRAY), number=10)} s')

        for ncons in [1,2,3,4,5,10,25,50,100]:
            for pfun in [do_nothing, average, long_computation_st, long_computation_mt]:

                print(f'{ncons} cons: {pfun.__name__}')
                
                for rep in tqdm(range(reps)):

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
                            
                        print(name + 40*'-')

                        fps_in, fps_out = run(
                            buffer = buf, 
                            processing_fun = pfun, 
                            t_measurement = 3, 
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

    g = sns.FacetGrid(timing_data, col="pfun", row="frame_sz")  
    g.map_dataframe(sns.lineplot, x="ncons", y="fps_out", hue="shm")
    plt.show()
