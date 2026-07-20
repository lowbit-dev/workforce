package window

import "sync"

// MuWindow is a concurrency-safe wrapper around Window.
// It uses a read-write mutex to ensure mechanical sympathy by allowing
// concurrent reads when no writes are occurring.
type MuWindow[T any] struct {
	mu sync.RWMutex
	w  *Window[T]
}

// NewMu creates a new concurrency-safe Window.
func NewMu[T any](capacity int) *MuWindow[T] {
	return &MuWindow[T]{
		w: New[T](capacity),
	}
}

// Push adds a new item to the window in a thread-safe manner.
func (mw *MuWindow[T]) Push(v T) {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	mw.w.Push(v)
}

// Each iterates over the elements in chronological order.
// NOTE: The read lock is held for the entire duration of the iteration.
// To maintain mechanical sympathy, ensure the provided callback is fast.
func (mw *MuWindow[T]) Each(fn func(T)) {
	mw.mu.RLock()
	defer mw.mu.RUnlock()
	mw.w.Each(fn)
}

// Average computes an average struct of type T safely across goroutines.
func (mw *MuWindow[T]) Average(add func(acc, next T) T, divide func(total T, count int) T) T {
	mw.mu.RLock()
	defer mw.mu.RUnlock()
	return mw.w.Average(add, divide)
}

// Count returns the current number of items safely.
func (mw *MuWindow[T]) Count() int {
	mw.mu.RLock()
	defer mw.mu.RUnlock()
	return mw.w.Count()
}
