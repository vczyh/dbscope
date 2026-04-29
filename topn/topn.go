// Package topn provides a generic, memory-bounded top-N tracker.
package topn

import "container/heap"

// New returns a Tracker that retains the N "largest" items per less in O(N) memory.
// less(a, b) reports whether a is smaller than b under the desired ordering;
// the tracker keeps the N items with the greatest ordering.
func New[T any](n int, less func(a, b T) bool) *Tracker[T] {
	return &Tracker[T]{n: n, h: &minHeap[T]{less: less}}
}

type Tracker[T any] struct {
	n int
	h *minHeap[T]
}

// Add inserts x into the tracker, evicting the smallest retained item once
// the buffer exceeds N.
func (t *Tracker[T]) Add(x T) {
	if t.n <= 0 {
		return
	}
	if t.h.Len() < t.n {
		heap.Push(t.h, x)
		return
	}
	if t.h.less(t.h.items[0], x) {
		t.h.items[0] = x
		heap.Fix(t.h, 0)
	}
}

// Drain empties the tracker and returns retained items in descending order.
func (t *Tracker[T]) Drain() []T {
	out := make([]T, t.h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(t.h).(T)
	}
	return out
}

type minHeap[T any] struct {
	items []T
	less  func(a, b T) bool
}

func (h *minHeap[T]) Len() int           { return len(h.items) }
func (h *minHeap[T]) Less(i, j int) bool { return h.less(h.items[i], h.items[j]) }
func (h *minHeap[T]) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *minHeap[T]) Push(x any)         { h.items = append(h.items, x.(T)) }
func (h *minHeap[T]) Pop() any {
	n := len(h.items)
	x := h.items[n-1]
	h.items = h.items[:n-1]
	return x
}