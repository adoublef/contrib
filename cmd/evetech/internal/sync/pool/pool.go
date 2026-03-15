package pool

import "sync"

type key[T any] struct{}

var bufferPools sync.Map

func Of[T any]( /* expression */ ) *sync.Pool {
	k := key[T]{}
	if p, ok := bufferPools.Load(k); ok {
		return p.(*sync.Pool)
	}
	pi, _ := bufferPools.LoadOrStore(k, &sync.Pool{
		New: func() any { return new(T) },
	})
	return pi.(*sync.Pool)
}

func Make[T any](n int) *sync.Pool {
	k := key[T]{}
	if p, ok := bufferPools.Load(k); ok {
		return p.(*sync.Pool)
	}
	pi, _ := bufferPools.LoadOrStore(k, &sync.Pool{
		New: func() any { return new(make([]T, n)) },
	})
	return pi.(*sync.Pool)
}
