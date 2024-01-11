from multiprocessing import RawArray
from typing import Tuple, List
from abc import ABC

from enum import Enum
class HeapType(Enum):
    MAXHEAP = 1
    MINHEAP = -1


def is_even(k):
    return (k & 1) == 0

class Heap(ABC):
    def push(self, item) -> None:
        pass

    def _siftup(self) -> None:
        pass

    def _siftdown(self) -> None:
        pass

    def clear(self) -> None:
        pass

class MinHeap(Heap):
    def __init__(self) -> None:
        self.heap = []

    def push(self, item) -> None:
        self.heap.append(item)
        self._siftdown(0, len(self.heap)-1)

    def pop(self):
        lastelt = self.heap.pop()  
        if self.heap:
            returnitem = self.heap[0]
            self.heap[0] = lastelt
            self._siftup(0)
            return returnitem
        return lastelt

    def _siftup(self, pos):
        endpos = len(self.heap)
        startpos = pos
        newitem = self.heap[pos]
        childpos = 2*pos + 1    # leftmost child position

        # Bubble up the smaller child until hitting a leaf.
        while childpos < endpos:
            # Set childpos to index of smaller child.
            rightpos = childpos + 1
            if rightpos < endpos and not self.heap[childpos] < self.heap[rightpos]:
                childpos = rightpos

            # Move the smaller child up.
            self.heap[pos] = self.heap[childpos]
            pos = childpos
            childpos = 2*pos + 1

        # The leaf at pos is empty now.  Put newitem there, and bubble it up
        # to its final resting place (by sifting its parents down).
        self.heap[pos] = newitem
        self._siftdown(startpos, pos)

    def _siftdown(self, startpos, pos):
        newitem = self.heap[pos]
        # Follow the path to the root, moving parents down until finding a place
        # newitem fits.
        while pos > startpos:
            parentpos = (pos - 1) >> 1
            parent = self.heap[parentpos]
            if newitem < parent:
                self.heap[pos] = parent
                pos = parentpos
                continue
            break
        self.heap[pos] = newitem

    def replace(self, item):
        returnitem = self.heap[0]    # raises appropriate IndexError if heap is empty
        self.heap[0] = item
        self._siftup(0)
        return returnitem

    def clear(self):
        self.heap = []

    def get_heap(self):
        return self.heap

class MaxHeap(Heap):
    def __init__(self):
        self.heap = []

    def push(self, item):
        self.heap.append(item)
        self._siftdown(0, len(self.heap)-1)

    def pop(self):
        lastelt = self.heap.pop()    # raises appropriate IndexError if heap is empty
        if self.heap:
            returnitem = self.heap[0]
            self.heap[0] = lastelt
            self._siftup(0)
            return returnitem
        return lastelt

    def _siftup(self, pos):
        endpos = len(self.heap)
        startpos = pos
        newitem = self.heap[pos]
        childpos = 2*pos + 1    # leftmost child position

        # Bubble up the larger child until hitting a leaf.
        while childpos < endpos:
            # Set childpos to index of larger child.
            rightpos = childpos + 1
            if rightpos < endpos and not self.heap[rightpos] < self.heap[childpos]:
                childpos = rightpos

            # Move the larger child up.
            self.heap[pos] = self.heap[childpos]
            pos = childpos
            childpos = 2*pos + 1

        # The leaf at pos is empty now.  Put newitem there, and bubble it up
        # to its final resting place (by sifting its parents down).
        self.heap[pos] = newitem
        self._siftdown(startpos, pos)

    def _siftdown(self, startpos, pos):
        newitem = self.heap[pos]
        # Follow the path to the root, moving parents down until finding a place
        # newitem fits.
        while pos > startpos:
            parentpos = (pos - 1) >> 1
            parent = self.heap[parentpos]
            if parent < newitem:
                self.heap[pos] = parent
                pos = parentpos
                continue
            break
        self.heap[pos] = newitem

    def replace(self, item):
        returnitem = self.heap[0]    # raises appropriate IndexError if heap is empty
        self.heap[0] = item
        self._siftup(0)
        return returnitem

    def clear(self):
        self.heap = []

    def get_heap(self):
        return self.heap
    
class MinMaxHeap(Heap):
    def __init__(self):
        self.heap = []

    def push(self, item):
        index = len(self.heap)
        self.heap.append(item)
        self._siftup(index)
        
    def pop_min(self):
        val = self.heap[0]
        last = self.heap.pop()
        if len(self.heap) > 0:
            self.heap[0] = last
            self._siftdown(0)
        return val

    def pop_max(self):
        size = len(self.heap)
        if size < 2:
            return self.pop_min()
        val = self.heap[1]
        last = self.heap.pop()
        if size > 2:
            self.heap[1] = last
            self._siftdown(1)
        return val

    def _siftup(self, pos):
        if is_even(pos):
            sibling = pos - 1 if (pos & 3) == 0 else pos >> 1
            if sibling > 0 and self.heap[sibling] < self.heap[pos]:
                self.heap[sibling], self.heap[pos] = self.heap[pos], self.heap[sibling]
                self._siftup_max(sibling)
            else:
                self._siftup_min(pos)
        else:
            if self.heap[pos-1] > self.heap[pos]:
                self.heap[pos-1], self.heap[pos] = self.heap[pos], self.heap[pos-1]
                self._siftup_min(pos-1)
            else:
                self._siftup_max(pos)

    def _siftup_min(self, pos):
        val = self.heap[pos]
        while pos > 0:
            parent = (pos >> 1) - 2 if (pos & 3) == 0 else (pos >> 1) - 1
            if self.heap[parent] <= val:
                break
            self.heap[pos] = self.heap[parent]
            pos = parent
        self.heap[pos] = val

    def _siftup_max(self, pos):
        val = self.heap[pos]
        while pos > 1:
            parent = pos >> 1 if (pos & 3) == 3 else (pos >> 1) - 1
            if self.heap[parent] >= val:
                break
            self.heap[pos] = self.heap[parent]
            pos = parent
        self.heap[pos] = val

    def _siftdown(self, pos):
        if is_even(pos):
            self._siftdown_min(pos)
        else:
            self._siftdown_max(pos)

    def _siftdown_min(self, pos):
        size = len(self.heap)
        if pos + 1 < size and self.heap[pos] > self.heap[pos+1]:
            self.heap[pos], self.heap[pos+1] = self.heap[pos+1], self.heap[pos]

        candidate, end = (pos + 1) << 1, min(size, (pos + 3) << 1)
        if candidate >= size:
            return

        for i in range(candidate+1, end):
            if self.heap[i] < self.heap[candidate]:
                candidate = i

        if self.heap[candidate] < self.heap[pos]:
            self.heap[pos], self.heap[candidate] = self.heap[candidate], self.heap[pos]
            self._siftdown(candidate)

    def _siftdown_max(self, pos):
        size = len(self.heap)
        if self.heap[pos-1] > self.heap[pos]:
            self.heap[pos-1], self.heap[pos] = self.heap[pos], self.heap[pos-1]

        candidate, end = pos << 1, min(size, (pos + 2) << 1)
        if candidate >= size:
            return
        
        for i in range(candidate+1, end):
            if self.heap[i] > self.heap[candidate]:
                candidate = i

        if self.heap[candidate] > self.heap[pos]:
            self.heap[pos], self.heap[candidate] = self.heap[candidate], self.heap[pos]
            self._siftdown(candidate)

    def replace_min(self, item):
        val = self.heap[0]
        self.heap[0] = item
        self._siftdown(0)
        return val

    def replace_max(self, item):
        size = len(self.heap)
        if size < 2:
            return self.replace_min(item)
        val = self.heap[1]
        self.heap[1] = item
        self._siftdown(1)
        return val

    def clear(self):
        self.heap = []

    def get_heap(self):
        return self.heap
    
class SharedMinHeap(Heap):

    def __init__(self, heapsize: int) -> None:
        self.heapsize = heapsize
        self.numel = 0
        self.heap = RawArray('d', self.heapsize)

    def get_heap_array(self) -> RawArray:
        return self.heap

    def clear(self):
        self.numel = 0

    def push(self, item): 
        self.heap[self.numel] = item
        self._siftdown(0, self.numel)
        self.numel += 1

    def pop(self):
        self.numel -= 1
        if self.numel < 0:
            self.numel = 0
            raise IndexError('pop from empty heap') 
        lastelt = self.heap[self.numel]    
        if self.numel > 0:
            returnitem = self.heap[0]
            self.heap[0] = lastelt
            self._siftup(0)
            return returnitem
        return lastelt

    def replace(self, item):
        """Pop and return the current smallest value, and add the new item."""
        if self.numel == 0:
            raise IndexError('replace from empty heap') 
        returnitem = self.heap[0]   
        self.heap[0] = item
        self._siftup(0)
        return returnitem


    def _siftdown(self, startpos, pos):
        newitem = self.heap[pos]
        # Follow the path to the root, moving parents down until finding a place
        # newitem fits.
        while pos > startpos:
            parentpos = (pos - 1) >> 1
            parent = self.heap[parentpos]
            if newitem < parent:
                self.heap[pos] = parent
                pos = parentpos
                continue
            break
        self.heap[pos] = newitem

    def _siftup(self, pos):
        endpos = self.numel
        startpos = pos
        newitem = self.heap[pos]
        childpos = 2*pos + 1    # leftmost child position

        # Bubble up the smaller child until hitting a leaf.
        while childpos < endpos:
            # Set childpos to index of smaller child.
            rightpos = childpos + 1
            if rightpos < endpos and not self.heap[childpos] < self.heap[rightpos]:
                childpos = rightpos

            # Move the smaller child up.
            self.heap[pos] = self.heap[childpos]
            pos = childpos
            childpos = 2*pos + 1

        # The leaf at pos is empty now.  Put newitem there, and bubble it up
        # to its final resting place (by sifting its parents down).
        self.heap[pos] = newitem
        self._siftdown(startpos, pos)

    def __str__(self):
        return str(list(self.heap[:self.numel]))
    
class SharedMaxHeap(Heap):

    def __init__(self, heapsize: int) -> None:
        self.heapsize = heapsize
        self.numel = 0
        self.heap = RawArray('d', self.heapsize)

    def get_heap_array(self) -> RawArray:
        return self.heap

    def clear(self):
        self.numel = 0

    def push(self, item):
        self.heap[self.numel] = item
        self._siftdown(0, self.numel)
        self.numel += 1

    def pop(self):
        self.numel -= 1
        if self.numel < 0:
            self.numel = 0
            raise IndexError('pop from empty heap') 
        lastelt = self.heap[self.numel]    
        if self.numel > 0:
            returnitem = self.heap[0]
            self.heap[0] = lastelt
            self._siftup(0)
            return returnitem
        return lastelt

    def replace(self, item):
        if self.numel == 0:
            raise IndexError('replace from empty heap') 
        returnitem = self.heap[0]    
        self.heap[0] = item
        self._siftup(0)
        return returnitem


    def _siftdown(self, startpos, pos):
        newitem = self.heap[pos]
        # Follow the path to the root, moving parents down until finding a place
        # newitem fits.
        while pos > startpos:
            parentpos = (pos - 1) >> 1
            parent = self.heap[parentpos]
            if parent < newitem:
                self.heap[pos] = parent
                pos = parentpos
                continue
            break
        self.heap[pos] = newitem    

    def _siftup(self, pos):
        endpos = self.numel
        startpos = pos
        newitem = self.heap[pos]
        childpos = 2*pos + 1    # leftmost child position

        # Bubble up the larger child until hitting a leaf.
        while childpos < endpos:
            # Set childpos to index of larger child.
            rightpos = childpos + 1
            if rightpos < endpos and not self.heap[rightpos] < self.heap[childpos]:
                childpos = rightpos

            # Move the larger child up.
            self.heap[pos] = self.heap[childpos]
            pos = childpos
            childpos = 2*pos + 1

        # The leaf at pos is empty now.  Put newitem there, and bubble it up
        # to its final resting place (by sifting its parents down).
        self.heap[pos] = newitem
        self._siftdown(startpos, pos)

    def __str__(self):
        return str(list(self.heap[:self.numel]))
    
class SharedMinMaxHeap(Heap):

    def __init__(self, heapsize: int) -> None:
        self.heapsize = heapsize
        self.numel = 0
        self.heap = RawArray('d', self.heapsize)

    def get_heap_array(self) -> RawArray:
        return self.heap
    
    def push(self, item):
        self.heap[self.numel] = item
        self._siftup(self.numel)
        self.numel += 1
        
    def pop_min(self):
        self.numel -= 1
        if self.numel < 0:
            self.numel = 0
            raise IndexError('pop from empty heap') 
      
        returnitem = self.heap[0]
        lastelt = self.heap[self.numel]   
        if self.numel > 0:
            self.heap[0] = lastelt
            self._siftdown(0)
        return returnitem

    def pop_max(self):
        if self.numel < 2:
            return self.pop_min()
        
        self.numel -= 1
        returnitem = self.heap[1]
        lastelt = self.heap[self.numel]    
        if self.numel > 2:
            self.heap[1] = lastelt
            self._siftdown(1)
        return returnitem

    def _siftup(self, pos):
        if is_even(pos):
            sibling = pos - 1 if (pos & 3) == 0 else pos >> 1
            if sibling > 0 and self.heap[sibling] < self.heap[pos]:
                self.heap[sibling], self.heap[pos] = self.heap[pos], self.heap[sibling]
                self._siftup_max(sibling)
            else:
                self._siftup_min(pos)
        else:
            if self.heap[pos-1] > self.heap[pos]:
                self.heap[pos-1], self.heap[pos] = self.heap[pos], self.heap[pos-1]
                self._siftup_min(pos-1)
            else:
                self._siftup_max(pos)

    def _siftup_min(self, pos):
        val = self.heap[pos]
        while pos > 0:
            parent = (pos >> 1) - 2 if (pos & 3) == 0 else (pos >> 1) - 1
            if self.heap[parent] <= val:
                break
            self.heap[pos] = self.heap[parent]
            pos = parent
        self.heap[pos] = val

    def _siftup_max(self, pos):
        val = self.heap[pos]
        while pos > 1:
            parent = pos >> 1 if (pos & 3) == 3 else (pos >> 1) - 1
            if self.heap[parent] >= val:
                break
            self.heap[pos] = self.heap[parent]
            pos = parent
        self.heap[pos] = val

    def _siftdown(self, pos):
        if is_even(pos):
            self._siftdown_min(pos)
        else:
            self._siftdown_max(pos)

    def _siftdown_min(self, pos):
        if pos + 1 < self.numel and self.heap[pos] > self.heap[pos+1]:
            self.heap[pos], self.heap[pos+1] = self.heap[pos+1], self.heap[pos]

        candidate, end = (pos + 1) << 1, min(self.numel, (pos + 3) << 1)
        if candidate >= self.numel:
            return

        for i in range(candidate+1, end):
            if self.heap[i] < self.heap[candidate]:
                candidate = i

        if self.heap[candidate] < self.heap[pos]:
            self.heap[pos], self.heap[candidate] = self.heap[candidate], self.heap[pos]
            self._siftdown(candidate)

    def _siftdown_max(self, pos):
        if self.heap[pos-1] > self.heap[pos]:
            self.heap[pos-1], self.heap[pos] = self.heap[pos], self.heap[pos-1]

        candidate, end = pos << 1, min(self.numel, (pos + 2) << 1)
        if candidate >= self.numel:
            return
        
        for i in range(candidate+1, end):
            if self.heap[i] > self.heap[candidate]:
                candidate = i

        if self.heap[candidate] > self.heap[pos]:
            self.heap[pos], self.heap[candidate] = self.heap[candidate], self.heap[pos]
            self._siftdown(candidate)

    def replace_min(self, item):
        val = self.heap[0]
        self.heap[0] = item
        self._siftdown(0)
        return val

    def replace_max(self, item):
        if self.numel < 2:
            return self.replace_min(item)
        val = self.heap[1]
        self.heap[1] = item
        self._siftdown(1)
        return val

    def clear(self):
        self.numel = 0

    def __str__(self):
        return str(list(self.heap[:self.numel]))

class SharedMinHeapTuple:
    def __init__(
        self, 
        heapsize: int, 
        tuplen: int = 1, 
        sortkey: int = 0, 
        ) -> None:
        
        if sortkey >= tuplen:
            raise ValueError('sortkey should be between 0 and tuplen-1')
            
        self.heapsize = heapsize
        self.tuplen = tuplen
        self.sortkey = sortkey
        self.numel = 0
        self.heap = RawArray('d', self.heapsize*self.tuplen)

    def get_heap_array(self) -> RawArray:
        return self.heap
    
    def get_tuple_elements(self, key: int = 0) -> List:
        selection = [self.heap[i*self.tuplen+key] for i in range(self.numel)]
        return selection
    
    def clear(self):
        self.numel = 0

    def push(self, item: Tuple) -> None:
        self.heap[self.numel*self.tuplen:(self.numel+1)*self.tuplen] = item
        self._siftdown_min(0, self.numel*self.tuplen)
        self.numel += 1

    def pop(self) -> Tuple:
        self.numel -= 1
        if self.numel < 0:
            self.numel = 0
            raise IndexError('pop from empty heap') 
        lastelt = tuple(self.heap[self.numel*self.tuplen:(self.numel+1)*self.tuplen])    
        if self.numel > 0:
            returnitem = tuple(self.heap[0:self.tuplen])
            self.heap[0:self.tuplen] = lastelt
            self._siftup(0)
            return returnitem
        return lastelt

    def replace(self, item: Tuple) -> Tuple:
        if self.numel == 0:
            raise IndexError('replace from empty heap') 
        returnitem = tuple(self.heap[0:self.tuplen])   
        self.heap[0:self.tuplen] = item
        self._siftup(0)
        return returnitem

    def _siftdown(self, startpos: int, pos: int) -> None:
        newitem = tuple(self.heap[pos:pos+self.tuplen])
        # Follow the path to the root, moving parents down until finding a place
        # newitem fits.
        while pos > startpos:
            parentpos = ((pos//self.tuplen - 1) >> 1)*self.tuplen 
            parent = tuple(self.heap[parentpos:parentpos+self.tuplen])
            if newitem[self.sortkey] < parent[self.sortkey]:
                self.heap[pos:pos+self.tuplen] = parent
                pos = parentpos
                continue
            break
        self.heap[pos:pos+self.tuplen] = newitem

    def _siftup(self, pos: int) -> None:
        endpos = self.numel*self.tuplen
        startpos = pos
        newitem = tuple(self.heap[pos:pos+self.tuplen])
        childpos = (2*pos//self.tuplen + 1)*self.tuplen    # leftmost child position

        # Bubble up the smaller child until hitting a leaf.
        while childpos < endpos:
            # Set childpos to index of smaller child.
            rightpos = (childpos//self.tuplen + 1) * self.tuplen
            if rightpos < endpos and not self.heap[childpos] < self.heap[rightpos]:
                childpos = rightpos
            # Move the smaller child up.
            self.heap[pos:pos+self.tuplen] = self.heap[childpos:childpos+self.tuplen]
            pos = childpos
            childpos = (2*pos//self.tuplen + 1)*self.tuplen

        # The leaf at pos is empty now.  Put newitem there, and bubble it up
        # to its final resting place (by sifting its parents down).
        self.heap[pos:pos+self.tuplen] = newitem
        self._siftdown(startpos, pos)

    def __str__(self):
        return str(list(self.heap[:self.numel*self.tuplen]))
    
class SharedMaxHeapTuple:
    def __init__(
        self, 
        heapsize: int, 
        tuplen: int = 1, 
        sortkey: int = 0, 
        ) -> None:
        
        if sortkey >= tuplen:
            raise ValueError('sortkey should be between 0 and tuplen-1')
            
        self.heapsize = heapsize
        self.tuplen = tuplen
        self.sortkey = sortkey
        self.numel = 0
        self.heap = RawArray('d', self.heapsize*self.tuplen)

    def get_heap_array(self) -> RawArray:
        return self.heap
    
    def get_tuple_elements(self, key: int = 0) -> List:
        selection = [self.heap[i*self.tuplen+key] for i in range(self.numel)]
        return selection
    
    def clear(self):
        self.numel = 0

    def push(self, item: Tuple) -> None:
        self.heap[self.numel*self.tuplen:(self.numel+1)*self.tuplen] = item
        self._siftdown(0, self.numel*self.tuplen)
        self.numel += 1

    def pop(self) -> Tuple:
        self.numel -= 1
        if self.numel < 0:
            self.numel = 0
            raise IndexError('pop from empty heap') 
        lastelt = tuple(self.heap[self.numel*self.tuplen:(self.numel+1)*self.tuplen])    
        if self.numel > 0:
            returnitem = tuple(self.heap[0:self.tuplen])
            self.heap[0:self.tuplen] = lastelt
            self._siftup(0)
            return returnitem
        return lastelt
    
    def replace(self, item: Tuple) -> Tuple:
        if self.numel == 0:
            raise IndexError('replace from empty heap') 
        returnitem = self.heap[0:self.tuplen]    
        self.heap[0:self.tuplen] = item
        self._siftup(0)
        return returnitem

    def _siftdown(self, startpos: int, pos: int) -> None:
        'Maxheap variant of _siftdown_min'
        newitem = self.heap[pos:pos+self.tuplen]
        # Follow the path to the root, moving parents down until finding a place
        # newitem fits.
        while pos > startpos:
            parentpos = ((pos//self.tuplen - 1) >> 1)*self.tuplen 
            parent = tuple(self.heap[parentpos:parentpos+self.tuplen])
            if parent[self.sortkey] < newitem[self.sortkey]:
                self.heap[pos:pos+self.tuplen] = parent
                pos = parentpos
                continue
            break
        self.heap[pos:pos+self.tuplen] = newitem

    def _siftup(self, pos):
        'Maxheap variant of _siftup_min'
        endpos = self.numel*self.tuplen
        startpos = pos
        newitem = tuple(self.heap[pos:pos+self.tuplen])
        childpos = (2*pos//self.tuplen + 1)*self.tuplen   # leftmost child position

        # Bubble up the larger child until hitting a leaf.
        while childpos < endpos:
            # Set childpos to index of larger child.
            rightpos = (childpos//self.tuplen + 1) * self.tuplen
            if rightpos < endpos and not self.heap[rightpos] < self.heap[childpos]:
                childpos = rightpos
            # Move the larger child up.
            self.heap[pos:pos+self.tuplen] = self.heap[childpos:childpos+self.tuplen]
            pos = childpos
            childpos = (2*pos//self.tuplen + 1)*self.tuplen

        # The leaf at pos is empty now.  Put newitem there, and bubble it up
        # to its final resting place (by sifting its parents down).
        self.heap[pos:pos+self.tuplen] = newitem
        self._siftdown(startpos, pos)

    def __str__(self):
        return str(list(self.heap[:self.numel*self.tuplen]))
    
class SharedMinMaxHeapTuple(Heap):
    def __init__(
        self, 
        heapsize: int, 
        tuplen: int = 1, 
        sortkey: int = 0, 
        ) -> None:
        
        if sortkey >= tuplen:
            raise ValueError('sortkey should be between 0 and tuplen-1')
            
        self.heapsize = heapsize
        self.tuplen = tuplen
        self.sortkey = sortkey
        self.numel = 0
        self.heap = RawArray('d', self.heapsize*self.tuplen)

    def get_heap_array(self) -> RawArray:
        return self.heap
    
    def get_tuple_elements(self, key: int = 0) -> List:
        selection = [self.heap[i*self.tuplen+key] for i in range(self.numel)]
        return selection
    
    def clear(self):
        self.numel = 0

    def push(self, item):
        self.heap[self.numel] = item
        self._siftup(self.numel)
        self.numel += 1
        
    def pop_min(self):
        self.numel -= 1
        if self.numel < 0:
            self.numel = 0
            raise IndexError('pop from empty heap') 
      
        returnitem = self.heap[0]
        lastelt = self.heap[self.numel]   
        if self.numel > 0:
            self.heap[0] = lastelt
            self._siftdown(0)
        return returnitem

    def pop_max(self):
        if self.numel < 2:
            return self.pop_min()
        
        self.numel -= 1
        returnitem = self.heap[1]
        lastelt = self.heap[self.numel]    
        if self.numel > 2:
            self.heap[1] = lastelt
            self._siftdown(1)
        return returnitem

    def _siftup(self, pos):
        if is_even(pos):
            sibling = pos - 1 if (pos & 3) == 0 else pos >> 1
            if sibling > 0 and self.heap[sibling] < self.heap[pos]:
                self.heap[sibling], self.heap[pos] = self.heap[pos], self.heap[sibling]
                self._siftup_max(sibling)
            else:
                self._siftup_min(pos)
        else:
            if self.heap[pos-1] > self.heap[pos]:
                self.heap[pos-1], self.heap[pos] = self.heap[pos], self.heap[pos-1]
                self._siftup_min(pos-1)
            else:
                self._siftup_max(pos)

    def _siftup_min(self, pos):
        val = self.heap[pos]
        while pos > 0:
            parent = (pos >> 1) - 2 if (pos & 3) == 0 else (pos >> 1) - 1
            if self.heap[parent] <= val:
                break
            self.heap[pos] = self.heap[parent]
            pos = parent
        self.heap[pos] = val

    def _siftup_max(self, pos):
        val = self.heap[pos]
        while pos > 1:
            parent = pos >> 1 if (pos & 3) == 3 else (pos >> 1) - 1
            if self.heap[parent] >= val:
                break
            self.heap[pos] = self.heap[parent]
            pos = parent
        self.heap[pos] = val

    def _siftdown(self, pos):
        if is_even(pos):
            self._siftdown_min(pos)
        else:
            self._siftdown_max(pos)

    def _siftdown_min(self, pos):
        if pos + 1 < self.numel and self.heap[pos] > self.heap[pos+1]:
            self.heap[pos], self.heap[pos+1] = self.heap[pos+1], self.heap[pos]

        candidate, end = (pos + 1) << 1, min(self.numel, (pos + 3) << 1)
        if candidate >= self.numel:
            return

        for i in range(candidate+1, end):
            if self.heap[i] < self.heap[candidate]:
                candidate = i

        if self.heap[candidate] < self.heap[pos]:
            self.heap[pos], self.heap[candidate] = self.heap[candidate], self.heap[pos]
            self._siftdown(candidate)

    def _siftdown_max(self, pos):
        if self.heap[pos-1] > self.heap[pos]:
            self.heap[pos-1], self.heap[pos] = self.heap[pos], self.heap[pos-1]

        candidate, end = pos << 1, min(self.numel, (pos + 2) << 1)
        if candidate >= self.numel:
            return
        
        for i in range(candidate+1, end):
            if self.heap[i] > self.heap[candidate]:
                candidate = i

        if self.heap[candidate] > self.heap[pos]:
            self.heap[pos], self.heap[candidate] = self.heap[candidate], self.heap[pos]
            self._siftdown(candidate)

    def replace_min(self, item):
        val = self.heap[0]
        self.heap[0] = item
        self._siftdown(0)
        return val

    def replace_max(self, item):
        if self.numel < 2:
            return self.replace_min(item)
        val = self.heap[1]
        self.heap[1] = item
        self._siftdown(1)
        return val

    def clear(self):
        self.numel = 0

    def __str__(self):
        return str(list(self.heap[:self.numel]))