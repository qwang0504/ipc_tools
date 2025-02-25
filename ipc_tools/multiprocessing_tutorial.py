import numpy as np
import multiprocessing
import time
import random
import os
from multiprocessing import Process, Lock, RLock, Value, Array

# All code derived from https://superfastpython.com/multiprocessing-in-python/ 

### Basic multiprocessing with function + arguments

# def some_function(t):
#     print('Process B before sleeping')
#     time.sleep(t)
#     print('Process B finished sleeping')


# if __name__ == '__main__':
#     process = Process(target=some_function, args=(10,)) #note that args expects a tuple
#     process.start()
#     print('Process A waiting for Process B to finish')
#     process.join()  



### Overriding run() method of multiprocessing.Process

# class CustomProcess(Process):

#     def __init__(self, t, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.t = t

#     def run(self):
#         print('Process B before sleeping')
#         time.sleep(self.t)
#         print('Process B after sleeping')

# if __name__ == '__main__':
#     process = CustomProcess(5)
#     process.start()
#     print('Process A waiting for Process B to finish')
#     process.join()  



### Sharing instance variables across processes with Array and Value

# class CustomProcess(Process):

#     def __init__(self, t, v, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.data = Value('i', v)
#         self.t = t

#     def run(self):
#         time.sleep(self.t)
#         self.data.value = 99
#         print(f'Process B value changed to {self.data.value}')

# if __name__ == '__main__':
#     t = 5
#     v = 0
#     current = multiprocessing.current_process()
#     print(current)
#     process = CustomProcess(t, v)
#     process.start()
#     print(process.name)
#     print(process.pid)
#     print('Process A waiting for Process B to finish')
#     print(f'Original value from Process A is {v}')
#     process.join()  
#     print(f'Modified value from Process B is {process.data.value}')



### Running multiple child processes 

# def child_process_task():
#     time.sleep(1)

# if __name__ == '__main__':
#     active_children = multiprocessing.active_children()
#     print(f'Number of active children is {len(active_children)}')
#     n_processes = input('Enter number of child processes to spawn: ')
#     processes = [Process(target=child_process_task) for _ in range(int(n_processes))]
#     for process in processes:
#         process.start()
#     active_children = multiprocessing.active_children()
#     print(f'Number of active children after starting is {len(active_children)}')
#     for child in active_children:
#         print(child)

#     num_cores = multiprocessing.cpu_count()
#     print(num_cores)



### Mutex locks

# def child_process_task(lock, identifier, sleep_time):
#     with lock: 
#         process_id = os.getpid()
#         print(process_id)
#         print(f'Process {identifier} acquired lock, sleeping for {sleep_time}')
#         time.sleep(sleep_time)

# if __name__ == '__main__':
#     n_processes = 10
#     lock = Lock()
#     processes = [Process(target=child_process_task, args=(lock, i, random.random())) for i in range(n_processes)]

#     for process in processes:
#         process.start()  #starts processes in order of i=0 to 10
#         print(f'Started process with PID {process.pid}') #prints each unique PID for processes arbitrarily assigned to 0-10

#     for process in processes:
#         process.join()



### Reentrant locks 

def child_process_task(lock, identifier, sleep_time):
    with lock: 
        process_id = os.getpid()
        print(process_id)
        print(f'Process {identifier} acquired lock, sleeping for {sleep_time}')
        time.sleep(sleep_time)
        report(lock, identifier) #RLock enables the same process to acquire the lock again

def report(lock, identifier):
    with lock: 
        process_id = os.getpid()
        print(process_id)
        print(f'Process {identifier, process_id} finished sleeping')

if __name__ == '__main__':
    n_processes = 10
    lock = RLock()
    processes = [Process(target=child_process_task, args=(lock, i, random.random())) for i in range(n_processes)]

    for process in processes:
        process.start()  #starts processes in order of i=0 to 10
        print(f'Started process with PID {process.pid}') #prints each unique PID for processes arbitrarily assigned to 0-10

    for process in processes:
        process.join()



### Events - process-safe boolean flags 





### Pipes and Queues


