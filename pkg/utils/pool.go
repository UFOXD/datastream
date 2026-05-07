package utils

// Pool is a generic, fixed-capacity object pool backed by a buffered channel.
//
// Get returns a pooled object when one is available and otherwise constructs a
// new value via the factory supplied to NewPool. Put returns an object to the
// pool, dropping it on the floor when the pool is full.
//
// Pool is safe for concurrent use.
type Pool[T any] struct {
	pool chan T
	new  func() T
}

// NewPool creates a Pool with the given capacity and factory function.
//
// If size is negative it is clamped to zero, in which case Get always invokes
// newFn and Put always drops the value.
func NewPool[T any](size int, newFn func() T) *Pool[T] {
	if size < 0 {
		size = 0
	}
	return &Pool[T]{
		pool: make(chan T, size),
		new:  newFn,
	}
}

// Get returns an object from the pool, allocating a new one when the pool is
// empty.
func (p *Pool[T]) Get() T {
	select {
	case obj := <-p.pool:
		return obj
	default:
		return p.new()
	}
}

// Put returns obj to the pool. If the pool is full the value is discarded.
func (p *Pool[T]) Put(obj T) {
	select {
	case p.pool <- obj:
	default:
	}
}
