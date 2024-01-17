import unittest
import numpy as np
from ipc_tools import RingBuffer

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

    def test_structured_array_0(self):
        dt = np.dtype([
            ('timestamp', np.float64, (1,)), 
            ('image', np.float32, SZ)
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

    
    def test_structured_array_1(self):
        dt = np.dtype([
            ('timestamp', np.float64, (1,)), 
            ('image', np.float32, SZ)
        ])

        Array_0 = np.random.uniform(low=-1, high=1, size=SZ).astype(np.float32)
        Array_1 = np.random.uniform(low=-1, high=1, size=SZ).astype(np.float32)

        x0 = np.array([(TS, Array_0)], dtype=dt) 
        x1 = np.array((TS, Array_1), dtype=dt) # no need for [] if only one element
        buf = RingBuffer(
            num_items=100,
            item_shape=(1,),
            data_type=dt
        )
        buf.put(x0)
        buf.put(x1)
        res0 = buf.get()
        res1 = buf.get()

        self.assertTrue(np.allclose(res0['image'], Array_0))
        self.assertTrue(np.allclose(res1['image'], Array_1))

if __name__ == '__main__':
    unittest.main()