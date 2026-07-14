package manager

import (
	"container/heap"
	"iter"
	"sync"
)

// Item wraps a generic value and maintains its index for heap operations.
type Item[T any] struct {
	Value T
	Index int
}

// PriorityQueue is a generic, concurrency-safe heap implementation.
type PriorityQueue[T any] struct {
	mu    sync.RWMutex
	items []*Item[T]
	// isHigherPriority returns true if 'a' should be popped before 'b'.
	isHigherPriority func(a, b T) bool
}

// NewPriorityQueue creates a new generic priority queue.
func NewPriorityQueue[T any](isHigherPriority func(a, b T) bool) *PriorityQueue[T] {
	return &PriorityQueue[T]{
		items:            make([]*Item[T], 0),
		isHigherPriority: isHigherPriority,
	}
}

// --- heap.Interface Implementation ---
// Note: These do NOT have locks because they are called by container/heap
// methods, which are invoked by our locked public methods.

func (pq *PriorityQueue[T]) Len() int { return len(pq.items) }

// Less bridges Go's required interface with our readable 3-AM-proof logic.
func (pq *PriorityQueue[T]) Less(i, j int) bool {
	return pq.isHigherPriority(pq.items[i].Value, pq.items[j].Value)
}

func (pq *PriorityQueue[T]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].Index = i
	pq.items[j].Index = j
}

func (pq *PriorityQueue[T]) Push(x any) {
	item := x.(*Item[T])
	item.Index = len(pq.items)
	pq.items = append(pq.items, item)
}

func (pq *PriorityQueue[T]) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]

	old[n-1] = nil  // Avoid memory leak
	item.Index = -1 // Mark as removed for safety
	pq.items = old[:n-1]

	return item
}

// PushItem adds a generic value to the queue safely.
func (pq *PriorityQueue[T]) PushItem(value T) *Item[T] {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	item := &Item[T]{Value: value}
	heap.Push(pq, item)
	return item
}

// PopItem removes and returns the highest priority value safely.
// Returns false if the queue is empty.
func (pq *PriorityQueue[T]) PopItem() (T, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.items) == 0 {
		var zero T
		return zero, false
	}

	item := heap.Pop(pq).(*Item[T])
	return item.Value, true
}

// Update adjusts the position of an item if its priority/value changes.
// It safely ignores items that have already been popped.
func (pq *PriorityQueue[T]) Update(item *Item[T], newValue T) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	// Ensure the item hasn't been popped by another goroutine (-1)
	// and is actually the item residing at that index.
	if item.Index < 0 || item.Index >= len(pq.items) || pq.items[item.Index] != item {
		return
	}

	item.Value = newValue
	heap.Fix(pq, item.Index)
}

// Size safely returns the number of items in the queue.
func (pq *PriorityQueue[T]) Size() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return len(pq.items)
}

func (pq *PriorityQueue[T]) Snapshot() []*Item[T] {
	pq.mu.RLock()
	snapshot := make([]*Item[T], len(pq.items))
	copy(snapshot, pq.items)
	pq.mu.RUnlock()

	return snapshot
}

// Values allows you to iterate over all the underlying values in the queue.
// Note: This iterates in the internal heap array order, NOT strict priority order.
func (pq *PriorityQueue[T]) Values() iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, item := range pq.Snapshot() {
			if !yield(item.Value) {
				return
			}
		}
	}
}

// Entries allows you to iterate over the wrapper Items.
// This is useful if you need to find a specific item to pass to pq.Update().
func (pq *PriorityQueue[T]) Entries() iter.Seq[*Item[T]] {
	return func(yield func(*Item[T]) bool) {
		for _, item := range pq.Snapshot() {
			if !yield(item) {
				return
			}
		}
	}
}
