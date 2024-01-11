import unittest
from ipc_tools import NormalHeap, SharedHeap, SharedHeapTuple, HeapType

class TestOrdering(unittest.TestCase):

    def test_ordering_normal_heap(self):
        h = NormalHeap(heaptype = HeapType.MINHEAP)
        h.push(0)
        h.push(5)
        h.push(2)
        h.push(10)
        h.push(-1)
        popped = []
        for i in range(5):
            popped.append(h.pop())
        self.assertEqual(popped, [-1,0,2,5,10])

if __name__ == '__main__':
    unittest.main()