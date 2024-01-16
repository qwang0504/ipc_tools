import unittest
import numpy as np
from ipc_tools import MonitoredQueue, RingBuffer

SZ = (1024, 1024)
TS = 10.0
ARRAY = np.random.uniform(low=-1, high=1, size=SZ).astype(np.float32)

class Tests(unittest.TestCase):

    def test_array(self):
        buf = RingBuffer(
            num_items=100,
            item_shape=SZ,
            data_type=np.float32
        )
        buf.put(ARRAY)
        res = buf.get()
        self.assertTrue(np.allclose(res, ARRAY))

    def test_structured_array(self):
        dt = np.dtype([
            ('timestamp', np.float64, (1,)), 
            ('image', np.float32, (1024,1024))
        ])
        x = np.array([(TS, ARRAY)], dtype=dt)
        buf = RingBuffer(
            num_items=100,
            item_shape=(1,),
            data_type=dt
        )
        buf.put(x)
        res = buf.get()
        self.assertEqual(TS, res['timestamp'])
        self.assertTrue(np.allclose(res['image'], x['image']))


if __name__ == '__main__':
    unittest.main()