import unittest
from ipc_tools import (
    MinHeap, MaxHeap, MinMaxHeap, 
    SharedMinHeap, SharedMaxHeap, SharedMinMaxHeap,
    SharedMinHeapTuple, SharedMaxHeapTuple, SharedMinMaxHeapTuple
)

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
        h = MinHeap()
        push_test_sequence(h)
        popped = []
        for i in range(len(TEST_SEQ)):
            popped.append(h.pop())
        self.assertEqual(popped, ASCENDING_TEST_SEQ)

    def test_ordering_normal_maxheap(self):
        h = MaxHeap()
        push_test_sequence(h)
        popped = []
        for i in range(len(TEST_SEQ)):
            popped.append(h.pop())
        self.assertEqual(popped, DESCENDING_TEST_SEQ)

    def test_ordering_normal_minmaxheap(self):
        h = MinMaxHeap()

        push_test_sequence(h)
        popped = []
        for i in range(len(TEST_SEQ)):
            popped.append(h.pop_min())

        push_test_sequence(h)
        popped = []
        for i in range(len(TEST_SEQ)):
            popped.append(h.pop_max())
        self.assertEqual(popped, DESCENDING_TEST_SEQ)

    def test_ordering_shared_minheap(self):
        h = SharedMinHeap(heapsize=10)
        push_test_sequence(h)
        popped = []
        for i in range(len(TEST_SEQ)):
            popped.append(h.pop())
        self.assertEqual(popped, ASCENDING_TEST_SEQ)

    def test_ordering_shared_maxheap(self):
        h = SharedMaxHeap(heapsize=10)
        push_test_sequence(h)
        popped = []
        for i in range(len(TEST_SEQ)):
            popped.append(h.pop())
        self.assertEqual(popped, DESCENDING_TEST_SEQ)

    def test_ordering_shared_minmaxheap(self):
        h = SharedMinMaxHeap(heapsize=10)

        push_test_sequence(h)
        popped = []
        for i in range(len(TEST_SEQ)):
            popped.append(h.pop_min())
        self.assertEqual(popped, ASCENDING_TEST_SEQ)

        push_test_sequence(h)
        popped = []
        for i in range(len(TEST_SEQ)):
            popped.append(h.pop_max())
        self.assertEqual(popped, DESCENDING_TEST_SEQ)

    def test_ordering_shared_tuple_minheap(self):
        h = SharedMinHeapTuple(heapsize=10, tuplen=2, sortkey=0)
        push_test_sequence_tuple(h)
        popped = []
        for i in range(len(TEST_TUPLE_SEQ)):
            popped.append(h.pop())
        self.assertEqual(popped, ASCENDING_TEST_TUPLE_SEQ_KEY0)

    def test_ordering_shared_tuple_maxheap(self):
        h = SharedMaxHeapTuple(heapsize=10, tuplen=2, sortkey=0)
        push_test_sequence_tuple(h)
        popped = []
        for i in range(len(TEST_TUPLE_SEQ)):
            popped.append(h.pop())
        self.assertEqual(popped, DESCENDING_TEST_TUPLE_SEQ_KEY0)

    def test_ordering_shared_tuple_minmaxheap(self):
        h = SharedMinMaxHeapTuple(heapsize=10, tuplen=2, sortkey=0)

        push_test_sequence_tuple(h)
        popped = []
        for i in range(len(TEST_TUPLE_SEQ)):
            popped.append(h.pop_min())
        self.assertEqual(popped, ASCENDING_TEST_TUPLE_SEQ_KEY0)

        push_test_sequence_tuple(h)
        popped = []
        for i in range(len(TEST_TUPLE_SEQ)):
            popped.append(h.pop_max())
        self.assertEqual(popped, DESCENDING_TEST_TUPLE_SEQ_KEY0)

    def test_ordering_shared_tuple_minheap_sortkey(self):
        h = SharedMinHeapTuple(heapsize=10, tuplen=2, sortkey=1)
        push_test_sequence_tuple(h)
        popped = []
        for i in range(len(TEST_TUPLE_SEQ)):
            popped.append(h.pop())
        self.assertEqual(popped, ASCENDING_TEST_TUPLE_SEQ_KEY1)

    def test_ordering_shared_tuple_maxheap_sortkey(self):
        h = SharedMaxHeapTuple(heapsize=10, tuplen=2, sortkey=1)
        push_test_sequence_tuple(h)
        popped = []
        for i in range(len(TEST_TUPLE_SEQ)):
            popped.append(h.pop())
        self.assertEqual(popped, DESCENDING_TEST_TUPLE_SEQ_KEY1)
    
    def test_ordering_shared_tuple_minmaxheap_sortkey(self):
        h = SharedMinMaxHeapTuple(heapsize=10, tuplen=2, sortkey=1)

        push_test_sequence_tuple(h)
        popped = []
        for i in range(len(TEST_TUPLE_SEQ)):
            popped.append(h.pop_min())
        self.assertEqual(popped, ASCENDING_TEST_TUPLE_SEQ_KEY1)

        push_test_sequence_tuple(h)
        popped = []
        for i in range(len(TEST_TUPLE_SEQ)):
            popped.append(h.pop_max())
        self.assertEqual(popped, DESCENDING_TEST_TUPLE_SEQ_KEY1)

if __name__ == '__main__':
    unittest.main()