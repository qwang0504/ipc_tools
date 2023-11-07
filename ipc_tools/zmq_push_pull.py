import zmq
import numpy as np
from typing import Optional
from numpy.typing import NDArray, ArrayLike, DTypeLike

class ZMQ_PushPull():
    def __init__(
            self,             
            item_shape: ArrayLike,
            data_type: DTypeLike,
            port: int = 5555,
            ipc: bool = False
        ):

        self.item_shape = np.asarray(item_shape)
        self.element_type = np.dtype(data_type)
        self.port = port
        self.ipc = ipc

    def initialize_sender(self):
        context = zmq.Context()
        self.sender = context.socket(zmq.PUSH)
        if self.ipc:
            id = f"ipc:///tmp/{self.port}"
        else:
            id = f"tcp://*:{self.port}"
        self.sender.bind(id)

    def initialize_receiver(self):
        context = zmq.Context()
        self.receiver = context.socket(zmq.PULL)
        if self.ipc:
            id = f"ipc:///tmp/{self.port}"
        else:
            id = f"tcp://localhost:{self.port}"
        self.receiver.connect(id)
        
    def put(self, element: ArrayLike) -> None:
        self.sender.send(element, copy=False)

    def get(self) -> Optional[NDArray]:
        return np.frombuffer(self.receiver.recv(), dtype=self.element_type).reshape(self.item_shape)
