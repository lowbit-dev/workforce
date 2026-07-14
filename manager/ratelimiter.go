package manager

import (
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Limiter implements a simple, allocation-free token bucket rate limiter.
type Limiter struct {
	mu          sync.Mutex
	rate        float64 // tokens added per second
	capacity    float64 // maximum burst capacity
	tokens      float64 // current available tokens
	lastUpdated time.Time
}

// New creates a new Limiter.
// It begins completely full, allowing an immediate burst up to the capacity.
func NewLimiter(rate float64, capacity float64) *Limiter {
	return &Limiter{
		rate:        rate,
		capacity:    capacity,
		tokens:      capacity,
		lastUpdated: time.Now(),
	}
}

// Allow returns true if a token is available, consuming it in the process.
// It evaluates time lazily to avoid the hidden state and overhead of background goroutines.
func (l *Limiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()

	// Calculate time elapsed since last check to replenish tokens
	elapsed := now.Sub(l.lastUpdated).Seconds()
	l.tokens += elapsed * l.rate

	// Enforce maximum capacity
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}
	l.lastUpdated = now

	// Consume a token if available
	if l.tokens >= 1.0 {
		l.tokens -= 1.0
		return true
	}

	return false
}

// =====================================================================
//  HTTP Wrapper
// =====================================================================

type HTTPLimiter struct {
	*Limiter
}

// New creates a new Limiter.
// It begins completely full, allowing an immediate burst up to the capacity.
func NewHTTPLimiter(rate float64, capacity float64) *HTTPLimiter {
	return &HTTPLimiter{Limiter: NewLimiter(rate, capacity)}
}

// Attempt evaluates the rate limit and returns the state required for HTTP headers.
// It returns whether the request is allowed, the remaining tokens, and the time until the bucket is fully reset.
func (l *HTTPLimiter) Attempt() (allowed bool, remaining int, reset time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastUpdated).Seconds()
	l.tokens += elapsed * l.rate

	// Enforce maximum capacity
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}
	l.lastUpdated = now

	// Consume a token if available
	allowed = l.tokens >= 1.0
	if allowed {
		l.tokens -= 1.0
	}

	remaining = int(l.tokens)

	// Calculate time until the bucket is completely full again
	timeToFullSecs := (l.capacity - l.tokens) / l.rate
	reset = time.Duration(timeToFullSecs * float64(time.Second))

	return allowed, remaining, reset
}

// Capacity returns the maximum burst size, useful for the RateLimit-Limit header.
func (l *HTTPLimiter) Capacity() int {
	return int(l.capacity)
}

func (l *HTTPLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowed, remaining, reset := l.Attempt()

		// Mechanical sympathy: strconv.Itoa is highly optimized and allocation-friendly.
		limitStr := strconv.Itoa(l.Capacity())
		remainingStr := strconv.Itoa(remaining)
		resetSecs := strconv.Itoa(int(math.Ceil(reset.Seconds())))

		// Inject standard informative headers
		w.Header().Set("RateLimit-Limit", limitStr)
		w.Header().Set("RateLimit-Remaining", remainingStr)
		w.Header().Set("RateLimit-Reset", resetSecs)

		if !allowed {
			// When rejecting, Retry-After is the universally respected standard
			w.Header().Set("Retry-After", resetSecs)
			http.Error(w, "429 Too Many Requests", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// =====================================================================
// LimiterCollection
// =====================================================================

type LimiterFactoryFunc func() *Limiter
type LimiterKeyFunc func(*http.Request) string

// LimiterCollection manages a dynamic set of HTTPLimiters, typically keyed by IP or API key.
type HTTPLimiterCollection struct {
	mu       sync.RWMutex
	limiters map[string]*HTTPLimiter
	factory  LimiterFactoryFunc
	keyfunc  LimiterKeyFunc
}

// NewCollection initializes a collection that mints limiters with the specified parameters.
func NewHTTPLimiterCollection(factory LimiterFactoryFunc, keyFunc LimiterKeyFunc) *HTTPLimiterCollection {
	return &HTTPLimiterCollection{
		limiters: make(map[string]*HTTPLimiter),
		factory:  factory,
		keyfunc:  keyFunc,
	}
}

// Get retrieves an existing limiter for the key, or provisions a new one if absent.
func (c *HTTPLimiterCollection) Get(key string) *HTTPLimiter {
	// Fast path: acquire read lock
	c.mu.RLock()
	limiter, exists := c.limiters[key]
	c.mu.RUnlock()

	if exists {
		return limiter
	}

	// Slow path: acquire write lock to provision a new limiter
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-checked locking to prevent race conditions during map insertion
	limiter, exists = c.limiters[key]
	if exists {
		return limiter
	}

	// Construct the decoupled HTTPLimiter directly.
	// Note: We assume 'tokens', 'lastUpdated' etc. are accessible within the same package.
	limiter = &HTTPLimiter{Limiter: c.factory()}
	c.limiters[key] = limiter
	return limiter
}

// Cleanup evicts limiters that have not been accessed within the provided idle duration.
// Lowbit design avoids magic: there is no hidden background goroutine. The caller
// owns the memory lifecycle and must call this explicitly (e.g., via a ticker).
func (c *HTTPLimiterCollection) Cleanup(idle time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, limiter := range c.limiters {
		// Acquire the individual limiter's lock to read its lastUpdated state safely
		limiter.mu.Lock()
		last := limiter.lastUpdated
		limiter.mu.Unlock()

		if now.Sub(last) > idle {
			delete(c.limiters, key)
		}
	}
}

var RemoteAddrKeyFunc = func(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return host
}

// Middleware enforces rate limits per-client using a key extraction function.
func (c *HTTPLimiterCollection) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limiter := c.Get(c.keyfunc(r))

		allowed, remaining, reset := limiter.Attempt()
		resetSecs := strconv.Itoa(int(math.Ceil(reset.Seconds())))

		w.Header().Set("RateLimit-Limit", strconv.Itoa(limiter.Capacity()))
		w.Header().Set("RateLimit-Remaining", strconv.Itoa(remaining))
		w.Header().Set("RateLimit-Reset", resetSecs)

		if !allowed {
			w.Header().Set("Retry-After", resetSecs)
			http.Error(w, "429 Too Many Requests", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}
