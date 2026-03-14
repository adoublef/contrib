package pool

import (
	"sync"
)

type key[T any] struct{}

var bufferPools sync.Map

func Of[T any]() *sync.Pool {
	var key key[T]
	if p, ok := bufferPools.Load(key); ok {
		return p.(*sync.Pool)
	}
	pi, _ := bufferPools.LoadOrStore(key, &sync.Pool{
		New: func() any { return new(T) },
	})
	return pi.(*sync.Pool)
}
