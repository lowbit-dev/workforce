package window

// Window represents a generic, fixed-size sliding window.
type Window[T any] struct {
	data  []T
	cap   int
	head  int
	count int
}

// New creates a new Window with the specified capacity.
func New[T any](capacity int) *Window[T] {
	if capacity <= 0 {
		panic("capacity must be greater than 0")
	}

	return &Window[T]{
		data: make([]T, capacity),
		cap:  capacity,
	}
}

// Push adds a new item to the window, overwriting the oldest if full.
func (w *Window[T]) Push(v T) {
	w.data[w.head] = v
	w.head = (w.head + 1) % w.cap
	if w.count < w.cap {
		w.count++
	}
}

// Each iterates over the elements in chronological order (oldest to newest).
func (w *Window[T]) Each(fn func(T)) {
	start := 0
	if w.count == w.cap {
		start = w.head
	}
	for i := 0; i < w.count; i++ {
		idx := (start + i) % w.cap
		fn(w.data[idx])
	}
}

// Average computes an average struct of type T.
// It requires two callbacks to stay zero alloc:
// 1. add: dictates how to sum two structs together.
// 2. divide: dictates how to divide the aggregated struct by the count.
func (w *Window[T]) Average(add func(acc, next T) T, divide func(total T, count int) T) T {
	var result T
	if w.count == 0 {
		return result
	}

	isFirst := true
	w.Each(func(v T) {
		if isFirst {
			result = v
			isFirst = false
		} else {
			result = add(result, v)
		}
	})

	return divide(result, w.count)
}

// Count returns the current number of items in the window.
func (w *Window[T]) Count() int {
	return w.count
}
