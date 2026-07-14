package manager

import (
	"iter"
	"sync"
)

// MuMap is a strongly-typed, concurrency-safe map.
// It avoids the hidden interface{} boxing allocations of sync.Map
// while providing straightforward, predictable behavior.
type MuMap[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

// New creates a new MuMap.
// It requires a capacity hint to prevent unnecessary map growth allocations,
// respecting the fact that we must know what we are asking the runtime to do.
func NewMuMap[K comparable, V any](capacity int) *MuMap[K, V] {
	if capacity < 0 {
		capacity = 0
	}
	return &MuMap[K, V]{
		m: make(map[K]V, capacity),
	}
}

// Load returns the value stored in the map for a key, or the zero value if no
// value is present. The ok result indicates whether the value was found.
func (m *MuMap[K, V]) Load(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.m[key]
	return v, ok
}

// Has performs an O(1) existence check for the key.
func (s *MuMap[K, V]) Has(key K) bool {
	s.mu.RLock()
	_, exists := s.m[key]
	s.mu.RUnlock()
	return exists
}

// Store sets the value for a key.
func (m *MuMap[K, V]) Store(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m[key] = value
}

// Delete removes the value for a key.
func (m *MuMap[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.m, key)
}

// Len returns the total number of items currently in the set.
func (s *MuMap[K, V]) Len() int {
	s.mu.RLock()
	count := len(s.m)
	s.mu.RUnlock()
	return count
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, Range stops the iteration.
// Note: Do not call Store or Delete inside the Range function, as it will deadlock.
func (m *MuMap[K, V]) Range(f func(key K, value V) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.m {
		if !f(k, v) {
			break
		}
	}
}

// Snapshot returns a shallow copy of the map.
// This gives the caller a point-in-time view of the data without
// holding locks indefinitely.
func (m *MuMap[K, V]) Snapshot() map[K]V {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Mechanical sympathy: Pre-allocate the exact capacity to avoid
	// rehashing and hidden allocations during the copy.
	snapshot := make(map[K]V, len(m.m))
	for k, v := range m.m {
		snapshot[k] = v
	}

	return snapshot
}

// Iterator returns an iter.Seq2 that yields key-value pairs.
// By yielding from a snapshot, we guarantee the mutex is not held
// during the caller's loop, eliminating the risk of deadlocks if
// the caller calls Store or Delete inside the loop.
func (m *MuMap[K, V]) Iterator() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for k, v := range m.Snapshot() {
			if !yield(k, v) {
				return
			}
		}
	}
}
