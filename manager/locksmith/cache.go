package locksmith

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

// Cache is a generic cache with support for LRU, TTL, and Quarantine.
// Functional options allow flexible configuration of the cache behavior.
//
// Usage Example:
//
//	cache := NewCache[int, string](
//	    WithCapacity(100),
//	    WithLRU(true),
//	    WithTTL(time.Minute),
//	)
//	cache.Put(1, "foo")
//	value, ok := cache.Get(1)
type Cache[K comparable, V any] struct {
	mu             sync.Mutex
	capacity       int
	cache          map[K]*list.Element
	lruList        *list.List
	ttl            time.Duration
	quarantineTTL  time.Duration
	quarantine     map[K]time.Time
	quarantineList *list.List
}

// entry stores the key-value pair and expiration time.
type entry[K comparable, V any] struct {
	key        K
	value      V
	expiry     time.Time // Expiry time for TTL
	quarantine bool      // Flag to indicate if the item is quarantined
}

// Option is a functional option for configuring the Cache.
type Option[K comparable, V any] func(*Cache[K, V])

// NewCache creates a new Cache with the provided options.
func NewCache[K comparable, V any](opts ...Option[K, V]) *Cache[K, V] {
	cache := &Cache[K, V]{
		capacity:       100,
		cache:          make(map[K]*list.Element),
		lruList:        list.New(),
		quarantine:     make(map[K]time.Time),
		quarantineList: list.New(),
		ttl:            time.Minute * 15,
		quarantineTTL:  time.Minute * 15,
	}

	for _, opt := range opts {
		opt(cache)
	}

	return cache
}

// WithCapacity sets the capacity of the cache.
func WithCapacity[K comparable, V any](capacity int) Option[K, V] {
	return func(c *Cache[K, V]) {
		c.capacity = capacity
	}
}

// WithLRU enables LRU eviction strategy when capacity is reached.
func WithLRU[K comparable, V any](enabled bool) Option[K, V] {
	return func(c *Cache[K, V]) {
		if enabled {
			// Enable LRU eviction behavior (handled in Put).
		}
	}
}

// WithTTL sets the time-to-live (TTL) for cache items.
func WithTTL[K comparable, V any](ttl time.Duration) Option[K, V] {
	return func(c *Cache[K, V]) {
		c.ttl = ttl
	}
}

// WithQuarantine sets the quarantine TTL for evicted items.
func WithQuarantine[K comparable, V any](quarantineTTL time.Duration) Option[K, V] {
	return func(c *Cache[K, V]) {
		c.quarantineTTL = quarantineTTL
	}
}

// Put inserts or updates an item in the cache and handles eviction, TTL, and quarantine.
func (c *Cache[K, V]) Put(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, found := c.cache[key]; found {
		elem.Value.(*entry[K, V]).value = value
		c.lruList.MoveToFront(elem)
		return
	}

	if len(c.cache) >= c.capacity {
		c.evict()
	}

	expiry := time.Now().Add(c.ttl)
	elem := c.lruList.PushFront(&entry[K, V]{key, value, expiry, false})
	c.cache[key] = elem
}

// Get retrieves an item from the cache, checking TTL expiration and quarantine.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, found := c.cache[key]; found {
		ent := elem.Value.(*entry[K, V])
		if time.Now().After(ent.expiry) {

			if !ent.quarantine {
				c.quarantineItem(key)
			}

			var zero V
			return zero, false // Expired, return zero value
		}

		c.lruList.MoveToFront(elem)
		return ent.value, true
	}

	var zero V
	return zero, false
}

// evict removes the least recently used (LRU) item from the cache.
func (c *Cache[K, V]) evict() {
	lruElem := c.lruList.Back()
	if lruElem != nil {
		c.lruList.Remove(lruElem)
		delete(c.cache, lruElem.Value.(*entry[K, V]).key)
	}
}

// quarantineItem puts an expired item into the quarantine map.
func (c *Cache[K, V]) quarantineItem(key K) {
	ent := c.cache[key].Value.(*entry[K, V])
	ent.quarantine = true
	c.quarantine[key] = time.Now()

	// Add to quarantine list for TTL
	c.quarantineList.PushFront(key)
}

// TODO:
func (c *Cache[K, V]) QuarantineExpiredItems() {
	for key, elem := range c.cache {
		ent := elem.Value.(*entry[K, V])
		if time.Now().After(ent.expiry) {
			if !ent.quarantine {
				c.quarantineItem(key)
			}
		}
	}
}

// QuarantineEvict removes an item from the quarantine after TTL expiry.
func (c *Cache[K, V]) QuarantineEvict() {
	for {
		c.mu.Lock()
		if c.quarantineList.Len() == 0 {
			c.mu.Unlock()
			break
		}

		elem := c.quarantineList.Back()
		if elem != nil {
			key := elem.Value.(K)
			if time.Now().After(c.quarantine[key].Add(c.quarantineTTL)) {
				// Quarantine TTL expired, fully evict the item
				c.quarantineList.Remove(elem)
				delete(c.cache, key)
				delete(c.quarantine, key)
			}
		}
		c.mu.Unlock()
	}
}

// Size returns the number of items currently in the cache.
func (c *Cache[K, V]) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cache)
}

func (c *Cache[K, V]) IncreaseTTL(key K) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, found := c.cache[key]; found {
		ent := elem.Value.(*entry[K, V])
		ent.expiry = time.Now().Add(c.ttl)

		return nil
	}

	return errors.New("unknown key")
}
