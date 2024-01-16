from ipc_tools import RingBuffer
import numpy as np

image = np.random.uniform(size=(1024,1024)).astype(np.float32)
ts = 10.0

# Non structured array 

buf_ns = RingBuffer(
    num_items=100,
    item_shape=(1024,1024),
    data_type=np.float32
)

buf_ns.put(image)
res = buf_ns.get()

# structured array

dt = np.dtype([
    ('timestamp', np.float64, (1,)), 
    ('image', np.float32, (1024,1024))
])

x = np.array([(ts, image)], dtype=dt)

buf_s = RingBuffer(
    num_items=100,
    item_shape=(1,),
    data_type=dt
)

buf_s.put(x)

res = buf_s.get()

buf_s.put(x)
buf_s.put(x)