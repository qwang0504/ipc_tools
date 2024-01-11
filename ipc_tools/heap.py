from multiprocessing import RawArray
from enum import Enum

class HeapType(Enum):
    MAXHEAP = -1,
    MINHEAP = 1,   

    # this is useful for argparse
    def __str__(self):
        return self.name
    
class NormalHeap:

    def __init__(self, heaptype: HeapType = HeapType.MINHEAP) -> None:
        self.heap = []
        self.heaptype = heaptype
        if heaptype == HeapType.MINHEAP:
            self.push = self._push_min
            self.pop = self._pop_min
            self.poppush = self._poppush_min
            self.pushpop = self._pushpop_min
        elif heaptype == HeapType.MAXHEAP:
            self.push = self._push_max
            self.pop =  self._pop_max
            self.poppush = self._poppush_max
            self.pushpop = self._pushpop_max
        else:
            raise ValueError('Unknown heap type')

    def _push_min(self, item):
        """Push item onto heap, maintaining the heap invariant."""
        self.heap.append(item)
        self._siftdown_min(0, len(self.heap)-1)

    def _push_max(self, item):
        """Push item onto heap, maintaining the heap invariant."""
        self.heap.append(item)
        self._siftdown_max(0, len(self.heap)-1)

    def _pop_min(self):
        """Pop the smallest item off the heap, maintaining the heap invariant."""
        lastelt = self.heap.pop()    # raises appropriate IndexError if heap is empty
        if self.heap:
            returnitem = self.heap[0]
            self.heap[0] = lastelt
            self._siftup_min(0)
            return returnitem
        return lastelt
    
    def _pop_max(self):
        """Maxheap version of a heappop."""
        lastelt = self.heap.pop()    # raises appropriate IndexError if heap is empty
        if self.heap:
            returnitem = self.heap[0]
            self.heap[0] = lastelt
            self._siftup_max(0)
            return returnitem
        return lastelt
    
    def _poppush_min(self, item):
        """Pop and return the current smallest value, and add the new item.

        This is more efficient than heappop() followed by heappush(), and can be
        more appropriate when using a fixed-size heap.  Note that the value
        returned may be larger than item!  That constrains reasonable uses of
        this routine unless written as part of a conditional replacement:

            if item > heap[0]:
                item = heapreplace(heap, item)
        """
        returnitem = self.heap[0]    # raises appropriate IndexError if heap is empty
        self.heap[0] = item
        self._siftup_min(0)
        return returnitem

    def _poppush_max(self, item):
        """Maxheap version of a heappop followed by a heappush."""
        returnitem = self.heap[0]    # raises appropriate IndexError if heap is empty
        self.heap[0] = item
        self._siftup_max(0)
        return returnitem
    
    def _pushpop_min(self, item):
        """Fast version of a heappush followed by a heappop."""
        if self.heap and self.heap[0] < item:
            item, self.heap[0] = self.heap[0], item
            self._siftup_min(0)
        return item

    def _pushpop_max(self, item):
        """Fast version of a heappush followed by a heappop."""
        if self.heap and self.heap[0] < item:
            item, self.heap[0] = self.heap[0], item
            self._siftup_max(0)
        return item

    # 'heap' is a heap at all indices >= startpos, except possibly for pos.  pos
    # is the index of a leaf with a possibly out-of-order value.  Restore the
    # heap invariant.
    def _siftdown_min(self, startpos, pos):
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

    def _siftdown_max(self, startpos, pos):
        'Maxheap variant of _siftdown_min'
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

    def _siftup_min(self, pos):
        endpos = len(self.heap)
        startpos = pos
        newitem = self.heap[pos]
        # Bubble up the smaller child until hitting a leaf.
        childpos = 2*pos + 1    # leftmost child position
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
        self._siftdown_min(startpos, pos)

    def _siftup_max(self, pos):
        'Maxheap variant of _siftup_min'
        endpos = len(self.heap)
        startpos = pos
        newitem = self.heap[pos]
        # Bubble up the larger child until hitting a leaf.
        childpos = 2*pos + 1    # leftmost child position
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
        self._siftdown_max(startpos, pos)

    def __str__(self):
        return str(self.heap)

class SharedHeap:

    def __init__(self, heapsize: int, heaptype: HeapType = HeapType.MINHEAP) -> None:
        self.heapsize = heapsize
        self.numel = 0
        self.heap = RawArray('d', self.heapsize)
        self.heaptype = heaptype
        if heaptype == HeapType.MINHEAP:
            self.push = self._push_min
            self.pop = self._pop_min
            self.poppush = self._poppush_min
            self.pushpop = self._pushpop_min
        elif heaptype == HeapType.MAXHEAP:
            self.push = self._push_max
            self.pop =  self._pop_max
            self.poppush = self._poppush_max
            self.pushpop = self._pushpop_max
        else:
            raise ValueError('Unknown heap type')

    def _push_min(self, item):
        """Push item onto heap, maintaining the heap invariant."""
        self.heap[self.numel] = item
        self._siftdown_min(0, self.numel)
        self.numel += 1

    def _push_max(self, item):
        """Push item onto heap, maintaining the heap invariant."""
        self.heap[self.numel] = item
        self._siftdown_max(0, self.numel)
        self.numel += 1

    def _pop_min(self):
        """Pop the smallest item off the heap, maintaining the heap invariant."""
        self.numel -= 1
        if self.numel < 0:
            self.numel = 0
            raise IndexError('pop from empty heap') 
        lastelt = self.heap[self.numel]    
        if self.numel > 0:
            returnitem = self.heap[0]
            self.heap[0] = lastelt
            self._siftup_min(0)
            return returnitem
        return lastelt
    
    def _pop_max(self):
        """Maxheap version of a heappop."""
        self.numel -= 1
        if self.numel < 0:
            self.numel = 0
            raise IndexError('pop from empty heap') 
        lastelt = self.heap[self.numel]    
        if self.numel > 0:
            returnitem = self.heap[0]
            self.heap[0] = lastelt
            self._siftup_max(0)
            return returnitem
        return lastelt

    def _pushpop_min(self, item):
        """Fast version of a heappush followed by a heappop."""
        if self.numel > 0 and self.heap[0] < item:
            item, self.heap[0] = self.heap[0], item
            self._siftup_min(0)
        return item
    
    def _pushpop_max(self, item): # not sure that works, please test 
        """Fast version of a heappush followed by a heappop."""
        if self.numel > 0 and self.heap[0] < item:
            item, self.heap[0] = self.heap[0], item
            self._siftup_max(0)
        return item
    
    def _poppush_min(self, item):
        """Pop and return the current smallest value, and add the new item."""
        if self.numel == 0:
            raise IndexError('replace from empty heap') 
        returnitem = self.heap[0]   
        self.heap[0] = item
        self._siftup_min(0)
        return returnitem
    
    def _poppush_max(self, item):
        """Maxheap version of a heappop followed by a heappush."""
        if self.numel == 0:
            raise IndexError('replace from empty heap') 
        returnitem = self.heap[0]    
        self.heap[0] = item
        self._siftup_max(0)
        return returnitem

    def _siftdown_min(self, startpos, pos):
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

    def _siftdown_max(self, startpos, pos):
        'Maxheap variant of _siftdown_min'
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

    def _siftup_min(self, pos):
        endpos = self.numel
        startpos = pos
        newitem = self.heap[pos]
        # Bubble up the smaller child until hitting a leaf.
        childpos = 2*pos + 1    # leftmost child position
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
        self._siftdown_min(startpos, pos)

    def _siftup_max(self, pos):
        'Maxheap variant of _siftup_min'
        endpos = self.numel
        startpos = pos
        newitem = self.heap[pos]
        # Bubble up the larger child until hitting a leaf.
        childpos = 2*pos + 1    # leftmost child position
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
        self._siftdown_max(startpos, pos)

    def __str__(self):
        return str(list(self.heap[:self.numel]))

from typing import Tuple
class SharedHeapTuple:
    def __init__(
            self, 
            heapsize: int, 
            tuplen: int = 1, 
            sortkey: int = 0, 
            heaptype: HeapType = HeapType.MINHEAP
        ) -> None:
        
        if sortkey >= tuplen:
            raise ValueError('sortkey should be between 0 and tuplen-1')
         
        self.heapsize = heapsize
        self.tuplen = tuplen
        self.sortkey = sortkey
        self.numel = 0
        self.heap = RawArray('d', self.heapsize*self.tuplen)
        self.heaptype = heaptype
        if heaptype == HeapType.MINHEAP:
            self.push = self._push_min
            self.pop = self._pop_min
            self.poppush = self._poppush_min
            self.pushpop = self._pushpop_min
        elif heaptype == HeapType.MAXHEAP:
            self.push = self._push_max
            self.pop =  self._pop_max
            self.poppush = self._poppush_max
            self.pushpop = self._pushpop_max
        else:
            raise ValueError('Unknown heap type')

    def _push_min(self, item: Tuple) -> None:
        """Push item onto heap, maintaining the heap invariant."""
        self.heap[self.numel*self.tuplen:(self.numel+1)*self.tuplen] = item
        self._siftdown_min(0, self.numel*self.tuplen)
        self.numel += 1

    def _push_max(self, item: Tuple) -> None:
        """Push item onto heap, maintaining the heap invariant."""
        self.heap[self.numel*self.tuplen:(self.numel+1)*self.tuplen] = item
        self._siftdown_max(0, self.numel*self.tuplen)
        self.numel += 1

    def _pop_min(self) -> Tuple:
        """Pop the smallest item off the heap, maintaining the heap invariant."""
        self.numel -= 1
        if self.numel < 0:
            self.numel = 0
            raise IndexError('pop from empty heap') 
        lastelt = tuple(self.heap[self.numel*self.tuplen:(self.numel+1)*self.tuplen])    
        if self.numel > 0:
            returnitem = tuple(self.heap[0:self.tuplen])
            self.heap[0:self.tuplen] = lastelt
            self._siftup_min(0)
            return returnitem
        return lastelt
    
    def _pop_max(self) -> Tuple:
        """Maxheap version of a heappop."""
        self.numel -= 1
        if self.numel < 0:
            self.numel = 0
            raise IndexError('pop from empty heap') 
        lastelt = tuple(self.heap[self.numel*self.tuplen:(self.numel+1)*self.tuplen])    
        if self.numel > 0:
            returnitem = tuple(self.heap[0:self.tuplen])
            self.heap[0:self.tuplen] = lastelt
            self._siftup_max(0)
            return returnitem
        return lastelt

    def _pushpop_min(self, item: Tuple) -> Tuple:
        """Fast version of a heappush followed by a heappop."""
        if self.numel > 0 and self.heap[0] < item[0]:
            item, self.heap[0:self.tuplen] = self.heap[0:self.tuplen], item
            self._siftup_min(0)
        return item
    
    def _pushpop_max(self, item: Tuple) -> Tuple: # not sure that works, please test 
        """Fast version of a heappush followed by a heappop."""
        if self.numel > 0 and self.heap[0] < item[0]:
            item, self.heap[0:self.tuplen] = self.heap[0:self.tuplen], item
            self._siftup_max(0)
        return item
    
    def _poppush_min(self, item: Tuple) -> Tuple:
        """Pop and return the current smallest value, and add the new item."""
        if self.numel == 0:
            raise IndexError('replace from empty heap') 
        returnitem = tuple(self.heap[0:self.tuplen])   
        self.heap[0:self.tuplen] = item
        self._siftup_min(0)
        return returnitem
    
    def _poppush_max(self, item: Tuple) -> Tuple:
        """Maxheap version of a heappop followed by a heappush."""
        if self.numel == 0:
            raise IndexError('replace from empty heap') 
        returnitem = self.heap[0:self.tuplen]    
        self.heap[0:self.tuplen] = item
        self._siftup_max(0)
        return returnitem

    def _siftdown_min(self, startpos: int, pos: int) -> None:
        newitem = tuple(self.heap[pos:pos+self.tuplen])
        # Follow the path to the root, moving parents down until finding a place
        # newitem fits.
        while pos > startpos:
            parentpos = (pos//self.tuplen - 1) >> 1
            parent = tuple(self.heap[parentpos:parentpos+self.tuplen])
            if newitem[self.sortkey] < parent[self.sortkey]:
                self.heap[pos:pos+self.tuplen] = parent
                pos = parentpos
                continue
            break
        self.heap[pos:pos+self.tuplen] = newitem

    def _siftdown_max(self, startpos: int, pos: int) -> None:
        'Maxheap variant of _siftdown_min'
        newitem = self.heap[pos:pos+self.tuplen]
        # Follow the path to the root, moving parents down until finding a place
        # newitem fits.
        while pos > startpos:
            parentpos = (pos//self.tuplen - 1) >> 1
            parent = tuple(self.heap[parentpos:parentpos+self.tuplen])
            if parent < newitem:
                self.heap[pos:pos+self.tuplen] = parent
                pos = parentpos
                continue
            break
        self.heap[pos:pos+self.tuplen] = newitem

    def _siftup_min(self, pos: int) -> None:
        endpos = self.numel*self.tuplen
        startpos = pos
        newitem = tuple(self.heap[pos:pos+self.tuplen])
        # Bubble up the smaller child until hitting a leaf.
        childpos = (2*pos//self.tuplen + 1)*self.tuplen    # leftmost child position
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
        self._siftdown_min(startpos, pos)

    def _siftup_max(self, pos):
        'Maxheap variant of _siftup_min'
        endpos = self.numel*self.tuplen
        startpos = pos
        newitem = tuple(self.heap[pos:pos+self.tuplen])
        # Bubble up the larger child until hitting a leaf.
        childpos = (2*pos//self.tuplen + 1)*self.tuplen   # leftmost child position
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
        self._siftdown_max(startpos, pos)

    def __str__(self):
        return str(list(self.heap[:self.numel*self.tuplen]))