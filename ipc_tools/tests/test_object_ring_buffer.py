import unittest
import numpy as np
from ipc_tools import ObjectRingBuffer, MonitoredQueue

SZ = (1024, 1024)
TS = 10.0
ARRAY = np.random.uniform(low=-1, high=1, size=SZ).astype(np.float32)

def serialize(obj):
    return obj

def deserialize(obj):
    return obj

def serialize2(obj):
    dt = np.dtype([
        ('index', np.float64, (1,)), 
        ('image', obj[1].dtype, obj[1].shape)
    ])
    arr = np.array([(obj[0], obj[1])], dtype=dt) 
    return arr

def deserialize2(arr):
    return [arr['index'], arr['image']]


class Tests(unittest.TestCase):

    def test_array(self):
        buf = ObjectRingBuffer(num_items=100, item_shape=SZ, serialize=serialize, deserialize=deserialize)
        buf.put(ARRAY)
        res = buf.get()
        self.assertTrue(np.allclose(res, ARRAY))

    def test_serialize(self):
        buf = ObjectRingBuffer(num_items=100, item_shape=SZ, serialize=serialize2, deserialize=deserialize2)
        buf.put([0,ARRAY])
        res = buf.get()
        print(res)

    def test_structured_array_0(self):
        dt = np.dtype([
            ('timestamp', np.float64, (1,)), 
            ('image', np.float32, SZ)
        ])
        x = np.array([(TS, ARRAY)], dtype=dt)
        buf = ObjectRingBuffer(num_items=100,serialize=serialize, deserialize=deserialize)
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
        x1 = np.array([(TS, Array_1)], dtype=dt) 
        buf = MonitoredQueue(ObjectRingBuffer(num_items=100, serialize=serialize, deserialize=deserialize))
        buf.put(x0)
        buf.put(x1)
        res0 = buf.get()
        res1 = buf.get()

        self.assertTrue(np.allclose(res0['image'], Array_0))
        self.assertTrue(np.allclose(res1['image'], Array_1))
        print(f'Freq in, freq out: {buf.get_average_freq()}') 

if __name__ == '__main__':
    unittest.main()