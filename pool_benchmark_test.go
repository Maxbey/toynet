package toynet

import (
	"math/bits"
	"testing"
)

var benchmarkBuffer *[]byte
var benchmarkBucket int

func BenchmarkMemoryPoolBucketSelection(b *testing.B) {
	tests := []struct {
		name string
		size int
	}{
		{name: "1_KiB", size: 1 << 10},
		{name: "5_KiB", size: 5 << 10},
		{name: "300_KiB", size: 300 << 10},
	}

	pool, err := newMemoryPool(2 << 20)
	if err != nil {
		b.Fatal(err)
	}
	for _, tt := range tests {
		b.Run("current_math_selector/"+tt.name, func(b *testing.B) {
			for b.Loop() {
				benchmarkBucket = pool.bucket(tt.size)
			}
		})

		b.Run("integer_logarithm_selector/"+tt.name, func(b *testing.B) {
			for b.Loop() {
				benchmarkBucket = bucketWithBits(tt.size)
			}
		})
	}
}

func BenchmarkMemoryPoolAcquire(b *testing.B) {
	tests := []struct {
		name   string
		size   int
		bucket int
	}{
		{name: "1_KiB", size: 1 << 10, bucket: 2},
		{name: "5_KiB", size: 5 << 10, bucket: 5},
		{name: "300_KiB", size: 300 << 10, bucket: 11},
	}

	for _, tt := range tests {
		b.Run("current_Acquire/"+tt.name, func(b *testing.B) {
			pool, err := newMemoryPool(2 << 20)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				buf, err := pool.Acquire(tt.size)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkBuffer = buf
				pool.buckets[tt.bucket].Put(buf)
			}
		})

		b.Run("temporary_Acquire_with_bits/"+tt.name, func(b *testing.B) {
			pool, err := newMemoryPool(2 << 20)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				buf := acquireWithBits(pool, tt.size)
				benchmarkBuffer = buf
				pool.buckets[tt.bucket].Put(buf)
			}
		})
	}
}

func acquireWithBits(pool *memoryPool, size int) *[]byte {
	bucket := bucketWithBits(size)
	if bucket == -1 {
		buf := make([]byte, size)
		return &buf
	}
	return pool.acquire(bucket)
}

func bucketWithBits(size int) int {
	if size <= 1<<shift {
		return 0
	}
	if size > 2<<20 {
		return -1
	}
	return bits.Len64(uint64(size)-1) - shift
}
