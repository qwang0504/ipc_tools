from ipc_tools import PriorityQueue
import numpy as np

# test basic functionality

SZ = (4,4)
ARRAY_0 = np.random.randint(0, 255, SZ, dtype=np.int32)
ARRAY_1 = np.random.randint(0, 255, SZ, dtype=np.int32)
ARRAY_2 = np.random.randint(0, 255, SZ, dtype=np.int32)

Q = PriorityQueue(        
        num_items = 100, 
        item_shape = SZ,
        data_type = np.int32
    )

Q.put((1, ARRAY_0))
Q.put((10, ARRAY_1))
Q.put((2, ARRAY_2))

print(Q)

assert(np.allclose(Q.get(), ARRAY_1))
assert(np.allclose(Q.get(), ARRAY_2))
assert(np.allclose(Q.get(), ARRAY_0))

# test multiprocessing

