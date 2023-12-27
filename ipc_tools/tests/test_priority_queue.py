from ipc_tools import PriorityQueue
import numpy as np

SZ = (4,4)
BIGARRAY = np.random.randint(0, 255, SZ, dtype=np.uint8)

Q = PriorityQueue(        
        num_items = 100, 
        item_shape = SZ,
        data_type = np.uint8
    )

Q.put((1, BIGARRAY))
Q.put((2, BIGARRAY))
Q.put((3, BIGARRAY))

print(Q)

Q.get()
Q.get()
Q.get()