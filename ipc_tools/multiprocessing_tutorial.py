import numpy as np
import multiprocessing
import time
import random
import os
from multiprocessing import Process, Lock, RLock, Value, Array, Event, Queue, Pipe
from queue import Empty

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

# def child_process_task(lock, identifier, sleep_time):
#     with lock: 
#         process_id = os.getpid()
#         print(process_id)
#         print(f'Process {identifier} acquired lock, sleeping for {sleep_time}')
#         time.sleep(sleep_time)
#         report(lock, identifier) #RLock enables the same process to acquire the lock again

# def report(lock, identifier):
#     with lock: 
#         process_id = os.getpid()
#         print(process_id)
#         print(f'Process {identifier, process_id} finished sleeping')

# if __name__ == '__main__':
#     n_processes = 10
#     lock = RLock()
#     processes = [Process(target=child_process_task, args=(lock, i, random.random())) for i in range(n_processes)]

#     for process in processes:
#         process.start()  #starts processes in order of i=0 to 10
#         print(f'Started process with PID {process.pid}') #prints each unique PID for processes arbitrarily assigned to 0-10

#     for process in processes:
#         process.join()



### Events - process-safe boolean flags 

# def event_task(event, number):
#     print(f'Process {number} waiting')
#     event.wait()
#     value = random.random()
#     time.sleep(value)
#     print(f'Process {number} got {value}')

# if __name__ == '__main__':
#     n_processes = 10
#     event = Event()
#     processes = [Process(target=event_task, args=(event, i)) for i in range(n_processes)]
#     for process in processes:
#         process.start()
#     time.sleep(1)
#     event.set()
#     for process in processes:
#         process.join()
    


### Queues

# def consumer(queue, t_refresh):
#     print('Consumer running...')
#     while True:
#         try:
#             index, item = queue.get(block=False)
#         except Empty:
#             print('Queue is empty, waiting for a while')
#             time.sleep(t_refresh)
#             continue
#         if item is None: 
#             break 
#         print(f'Consumer got item number {index}: {item}')
#     print('Consumer finished')
    

# def producer(queue):
#     for i in range(10):
#         value = random.random()
#         time.sleep(value)
#         queue.put((i, value))
#     queue.put((i+1, None))
#     print('Producer finished')


# if __name__ == '__main__':
#     queue = Queue()
#     consumer_process = Process(target=consumer, args=(queue, 0.1))
#     producer_process = Process(target=producer, args=(queue,))

#     consumer_process.start()
#     producer_process.start()

#     consumer_process.join()
#     producer_process.join()



### Pipes 

## conn1, conn2 = Pipe() --> conn1 is only receiving 

# def sender(connection):
#     print("Sender started")
#     for i in range(10):
#         value = random.random()
#         time.sleep(value)
#         connection.send(value)
#     connection.send(None)
#     print('Sender complete')
    
# def receiver(connection):
#     print("Receiver running")
#     while True:
#         item = connection.recv()
#         print(f"Receiver got {item}")
#         if item == None:
#             break
#     print("Receiver complete")


## Pipe(duplex=True) is bidirectional

def generate_value(connection, value):
    new_value = random.random()    
    time.sleep(new_value)
    value = value + new_value 
    print(f"Sending {value}")
    connection.send(value)
    
def pingpong(connection, send_first):
    if send_first:
        generate_value(connection, 0)
    while True: 
        value = connection.recv()
        print(f'Received {value}')
        generate_value(connection, value)
        if value > 10:
            break 
    print("Process complete")


if __name__ == '__main__':
    # conn1, conn2 = Pipe() 
    # sender_process = Process(target=sender, args=(conn2,))
    # receiver_process = Process(target=receiver, args=(conn1,))

    # receiver_process.start()
    # sender_process.start()

    # receiver_process.join()
    # sender_process.join()

    conn1, conn2 = Pipe(duplex=True)
    p1 = Process(target=pingpong, args=(conn1, True))
    p2 = Process(target=pingpong, args=(conn2, False))

    p1.start()
    p2.start()

    p1.join()
    p2.join()