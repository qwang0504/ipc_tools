import unittest
from ipc_tools import NormalHeap, SharedHeap, SharedHeapTuple, HeapType

TEST_SEQ = [0,5,2,10,-1]
TEST_TUPLE_SEQ = [
    (0,-10),
    (5,20),
    (2,10),
    (10,11),
    (-1,3)
]

ASCENDING_TEST_SEQ = sorted(TEST_SEQ)
DESCENDING_TEST_SEQ = sorted(TEST_SEQ, reverse=True)
ASCENDING_TEST_TUPLE_SEQ_KEY0 = sorted(TEST_TUPLE_SEQ, key=lambda x: x[0])
DESCENDING_TEST_TUPLE_SEQ_KEY0 = sorted(TEST_TUPLE_SEQ, key=lambda x: x[0], reverse=True)
ASCENDING_TEST_TUPLE_SEQ_KEY1 = sorted(TEST_TUPLE_SEQ, key=lambda x: x[1])
DESCENDING_TEST_TUPLE_SEQ_KEY1 = sorted(TEST_TUPLE_SEQ, key=lambda x: x[1], reverse=True)

def push_test_sequence(h) -> None:
    for elt in TEST_SEQ:
        h.push(elt)

def push_test_sequence_tuple(h) -> None:
    for elt in TEST_TUPLE_SEQ:
        h.push(elt)

class TestOrdering(unittest.TestCase):

    def test_ordering_normal_minheap(self):
        h = NormalHeap(heaptype = HeapType.MINHEAP)
        push_test_sequence(h)
        popped = []
        for i in range(5):
            popped.append(h.pop())
        self.assertEqual(popped, ASCENDING_TEST_SEQ)

    def test_ordering_normal_maxheap(self):
        h = NormalHeap(heaptype = HeapType.MAXHEAP)
        push_test_sequence(h)
        popped = []
        for i in range(5):
            popped.append(h.pop())
        self.assertEqual(popped, DESCENDING_TEST_SEQ)

    def test_ordering_shared_minheap(self):
        h = SharedHeap(heapsize=10, heaptype = HeapType.MINHEAP)
        push_test_sequence(h)
        popped = []
        for i in range(5):
            popped.append(h.pop())
        self.assertEqual(popped, ASCENDING_TEST_SEQ)

    def test_ordering_shared_maxheap(self):
        h = SharedHeap(heapsize=10, heaptype = HeapType.MAXHEAP)
        push_test_sequence(h)
        popped = []
        for i in range(5):
            popped.append(h.pop())
        self.assertEqual(popped, DESCENDING_TEST_SEQ)

    def test_ordering_shared_tuple_minheap(self):
        h = SharedHeapTuple(heapsize=10, tuplen=2, sortkey=0, heaptype = HeapType.MINHEAP)
        push_test_sequence_tuple(h)
        popped = []
        for i in range(5):
            popped.append(h.pop())
        self.assertEqual(popped, ASCENDING_TEST_TUPLE_SEQ_KEY0)

    def test_ordering_shared_tuple_maxheap(self):
        h = SharedHeapTuple(heapsize=10, tuplen=2, sortkey=0, heaptype = HeapType.MAXHEAP)
        push_test_sequence_tuple(h)
        popped = []
        for i in range(5):
            popped.append(h.pop())
        self.assertEqual(popped, DESCENDING_TEST_TUPLE_SEQ_KEY0)

    def test_ordering_shared_tuple_minheap_sortkey(self):
        h = SharedHeapTuple(heapsize=10, tuplen=2, sortkey=1, heaptype = HeapType.MINHEAP)
        push_test_sequence_tuple(h)
        popped = []
        for i in range(5):
            popped.append(h.pop())
        self.assertEqual(popped, ASCENDING_TEST_TUPLE_SEQ_KEY1)

    def test_ordering_shared_tuple_maxheap_sortkey(self):
        h = SharedHeapTuple(heapsize=10, tuplen=2, sortkey=1, heaptype = HeapType.MAXHEAP)
        push_test_sequence_tuple(h)
        popped = []
        for i in range(5):
            popped.append(h.pop())
        self.assertEqual(popped, DESCENDING_TEST_TUPLE_SEQ_KEY1)

if __name__ == '__main__':
    unittest.main()