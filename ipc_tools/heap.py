# This code has been extensively copied from the heapq module

# self.heap.pop 
# self.heap.append

class Heap:

    def __init__(self):
        self.heap = []

    def push(self, item):
        """Push item onto heap, maintaining the heap invariant."""
        self.heap.append(item)
        self._siftdown(0, len(self.heap)-1)

    def pop(self):
        """Pop the smallest item off the heap, maintaining the heap invariant."""
        lastelt = self.heap.pop()    # raises appropriate IndexError if heap is empty
        if self.heap:
            returnitem = self.heap[0]
            self.heap[0] = lastelt
            self._siftup(0)
            return returnitem
        return lastelt

    def replace(self, item):
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
        self._siftup(0)
        return returnitem

    def pushpop(self, item):
        """Fast version of a heappush followed by a heappop."""
        if self.heap and self.heap[0] < item:
            item, self.heap[0] = self.heap[0], item
            self._siftup(0)
        return item

    def _pop_max(self):
        """Maxheap version of a heappop."""
        lastelt = self.heap.pop()    # raises appropriate IndexError if heap is empty
        if self.heap:
            returnitem = self.heap[0]
            self.heap[0] = lastelt
            self._siftup_max(0)
            return returnitem
        return lastelt

    def _replace_max(self, item):
        """Maxheap version of a heappop followed by a heappush."""
        returnitem = self.heap[0]    # raises appropriate IndexError if heap is empty
        self.heap[0] = item
        self._siftup_max(0)
        return returnitem

    # 'heap' is a heap at all indices >= startpos, except possibly for pos.  pos
    # is the index of a leaf with a possibly out-of-order value.  Restore the
    # heap invariant.
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
        self._siftdown(startpos, pos)

    def _siftdown_max(self, startpos, pos):
        'Maxheap variant of _siftdown'
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

    def _siftup_max(self, pos):
        'Maxheap variant of _siftup'
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
