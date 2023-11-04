import zmq
import numpy as np
from typing import Optional
from numpy.typing import NDArray, ArrayLike, DTypeLike

class ZMQ_PushPull():
    def __init__(
            self,             
            item_shape: ArrayLike,
            data_type: DTypeLike,
            port: int = 5555
        ):

        self.item_shape = np.asarray(item_shape)
        self.element_type = np.dtype(data_type)
        self.port = port

    def initialize_sender(self):
        context = zmq.Context()
        self.sender = context.socket(zmq.PUSH)
        self.sender.bind("tcp://*:" + str(self.port))

    def initialize_receiver(self):
        context = zmq.Context()
        self.receiver = context.socket(zmq.PULL)
        self.receiver.connect("tcp://localhost:" + str(self.port))
        
    def put(self, element: ArrayLike) -> None:
        self.sender.send(element)

    def get(self) -> Optional[NDArray]:
        return np.frombuffer(self.receiver.recv(), dtype=self.element_type).reshape(self.item_shape)
