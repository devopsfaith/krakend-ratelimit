package krakendrate

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkShardedMemoryBackend_Load(b *testing.B) {
	benchmarkBackendsLoad(b, defaultShardedMemoryBackend)
}

func BenchmarkShardedMemoryBackend_LoadParallel(b *testing.B) {
	benchmarkBackendsLoadParallel(b, defaultShardedMemoryBackend)
}

func BenchmarkMemoryBackend_Load(b *testing.B) {
	benchmarkBackendsLoad(b, memoryBackend)
}

func BenchmarkMemoryBackend_LoadParallel(b *testing.B) {
	benchmarkBackendsLoadParallel(b, memoryBackend)
}

func defaultShardedMemoryBackend(ctx context.Context) Backend {
	return DefaultShardedMemoryBackend(ctx)
}

func memoryBackend(ctx context.Context) Backend {
	return NewMemoryBackend(ctx, DataTTL, nil)
}

func benchmarkBackendsLoad(b *testing.B, f func(context.Context) Backend) {
	for _, max := range []int{0, 100, 5000, 50000, 1000000, 10000000} {
		b.Run(fmt.Sprintf("%d-elements", max), func(b *testing.B) {
			benchmarkBackendLoad(b, max, f)
		})
	}
}

func benchmarkBackendsLoadParallel(b *testing.B, f func(context.Context) Backend) {
	for _, max := range []int{0, 100, 5000, 50000, 1000000, 10000000} {
		b.Run(fmt.Sprintf("%d-elements", max), func(b *testing.B) {
			benchmarkBackendLoadParallel(b, max, f)
		})
	}
}

func benchmarkBackendLoad(b *testing.B, max int, f func(context.Context) Backend) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backend := f(ctx)

	for i := 0; i < max; i++ {
		backend.Store(fmt.Sprintf("key-%d", i), DummyLimiter(i))
	}

	var v interface{}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("key-%d", i)
		v = backend.Load(k, func() Limiter { return nil })
	}
	loadResult = v
}

func benchmarkBackendLoadParallel(b *testing.B, max int, f func(context.Context) Backend) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backend := f(ctx)

	for i := 0; i < max; i++ {
		backend.Store(fmt.Sprintf("key-%d", i), DummyLimiter(i))
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var v interface{}
		i := 0
		for pb.Next() {
			k := fmt.Sprintf("key-%d", i)
			v = backend.Load(k, func() Limiter { return nil })
			i++
		}
		loadResult = v
	})
}

var loadResult interface{}
