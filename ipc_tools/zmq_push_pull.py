import zmq
import numpy as np
from typing import Optional, Any
from numpy.typing import NDArray, ArrayLike, DTypeLike
from .queue_like import QueueLike
from queue import Full, Empty

class ZMQ_PushPull(QueueLike):

    def __init__(
            self,             
            port: int = 5555,
            ipc: bool = False
        ):

        self.port = port
        self.ipc = ipc
        self.sender_initialized = False
        self.receiver_initialized = False

    def initialize_sender(self):
        context = zmq.Context()
        self.sender = context.socket(zmq.PUSH)

        if self.ipc:
            id = f"ipc:///tmp/{self.port}"
        else:
            id = f"tcp://*:{self.port}"

        self.sender.bind(id)
        self.pollout = zmq.Poller()
        self.pollout.register(self.sender, zmq.POLLOUT)
        self.sender_initialized = True

    def initialize_receiver(self):
        context = zmq.Context()
        self.receiver = context.socket(zmq.PULL)

        if self.ipc:
            id = f"ipc:///tmp/{self.port}"
        else:
            id = f"tcp://localhost:{self.port}"

        self.receiver.connect(id)
        self.pollin = zmq.Poller()
        self.pollin.register(self.receiver, zmq.POLLIN)
        self.receiver_initialized = True

    def qsize(self):
        pass

    def empty(self) -> bool:
        pass
    
    def full(self) -> bool:
        pass

    def close(self) -> bool:
        pass

    def clear(self) -> None:
        pass

class ZMQ_PushPullArray(ZMQ_PushPull, QueueLike):

    def __init__(
            self,             
            item_shape: ArrayLike,
            data_type: DTypeLike,
            port: int = 5555,
            ipc: bool = False
        ):

        super().__init__(port = port, ipc = ipc)

        self.item_shape = np.asarray(item_shape)
        self.element_type = np.dtype(data_type)
        
    def put(self, element: ArrayLike, block: Optional[bool] = True, timeout: Optional[float] = None) -> None:
        if not self.sender_initialized:
            self.initialize_sender()
        
        try:
            if block:
                socks = dict(self.pollout.poll(timeout))
                if self.sender in socks and socks[self.sender] == zmq.POLLOUT:
                    self.sender.send(element, copy=False)
                else:
                    raise zmq.ZMQError
            else:
                self.sender.send(element, flags = zmq.NOBLOCK, copy=False)
        except zmq.ZMQError:
            raise Full

    def get(self, block: Optional[bool] = True, timeout: Optional[float] = None) -> Optional[NDArray]:
        if not self.receiver_initialized:
            self.initialize_receiver()

        try:
            if block:
                socks = dict(self.pollin.poll(timeout))
                if self.receiver in socks and socks[self.receiver] == zmq.POLLIN:
                    res =  np.frombuffer(
                        self.receiver.recv(), 
                        dtype=self.element_type
                    ).reshape(self.item_shape)
                else:
                    raise zmq.ZMQError
            else:
                res =  np.frombuffer(
                    self.receiver.recv(flags = zmq.NOBLOCK), 
                    dtype=self.element_type
                ).reshape(self.item_shape)

            return res
        
        except zmq.ZMQError:
            raise Empty 
    
class ZMQ_PushPullObj(ZMQ_PushPull, QueueLike):
        
    def put(self, element: Any, block: Optional[bool] = True, timeout: Optional[float] = None) -> None:
        if not self.sender_initialized:
            self.initialize_sender()

        try:
            if block:
                socks = dict(self.pollout.poll(timeout))
                if self.sender in socks and socks[self.sender] == zmq.POLLOUT:
                    self.sender.send_pyobj(element, copy=False)
                else:
                    raise zmq.ZMQError
            else:
                self.sender.send_pyobj(element, flags = zmq.NOBLOCK, copy=False)

        except zmq.ZMQError:
            raise Full

    def get(self, block: Optional[bool] = True, timeout: Optional[float] = None) -> Optional[Any]:
        if not self.receiver_initialized:
            self.initialize_receiver()
        
        try:
            if block:
                socks = dict(self.pollin.poll(timeout))
                if self.receiver in socks and socks[self.receiver] == zmq.POLLIN:
                    res = self.receiver.recv_pyobj()
                else:
                    raise zmq.ZMQError
            else:
                res = self.receiver.recv_pyobj(flags = zmq.NOBLOCK)

            return res
        
        except zmq.ZMQError:
            raise Empty 
        