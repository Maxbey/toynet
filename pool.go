package toynet

import (
	"fmt"
	"math/bits"
	"sync"
)

const (
	buckets = 16
	shift   = 8
)

type memoryPool struct {
	buckets []sync.Pool
	maxBuf  int
}

func newMemoryPool(maxBuf int) (*memoryPool, error) {
	if maxBuf < 1<<shift {
		return nil, fmt.Errorf("maximum buffer size %d is below minimum %d", maxBuf, 1<<shift)
	}

	pool := &memoryPool{
		buckets: make([]sync.Pool, 0),
		maxBuf:  maxBuf,
	}

	size := 1 << shift
	for size <= maxBuf {
		s := size
		pool.buckets = append(pool.buckets, sync.Pool{
			New: func() any {
				b := make([]byte, s)
				return &b
			},
		})

		size *= 2
	}

	return pool, nil
}

func (m *memoryPool) Acquire(size int) (*[]byte, error) {
	if size <= 0 {
		return nil, fmt.Errorf("acquiring buffer <= 0: %d", size)
	}

	if size > m.maxBuf {
		return nil, fmt.Errorf("requested buffer size %d exceeds maximum %d", size, m.maxBuf)
	}

	bucket := m.bucket(size)

	if bucket == -1 {
		b := make([]byte, size)
		return &b, nil
	}

	return m.acquire(bucket), nil

}

func (m *memoryPool) Release(buf *[]byte) error {
	if buf == nil {
		return fmt.Errorf("buffer is nil")
	}
	if cap(*buf) < (1 << shift) {
		return fmt.Errorf("buffer capacity %d is below minimum %d", cap(*buf), 1<<shift)
	}
	if cap(*buf) > 1<<(len(m.buckets)+shift-1) {
		return nil
	}

	bucket := m.bucket(cap(*buf))
	if cap(*buf) != 1<<(bucket+shift) {
		return fmt.Errorf("buffer capacity %d does not match a size class", cap(*buf))
	}
	m.release(buf, bucket)

	return nil
}

func (m *memoryPool) acquire(bucket int) *[]byte {
	return m.buckets[bucket].Get().(*[]byte)
}

func (m *memoryPool) release(buf *[]byte, bucket int) {
	*buf = (*buf)[:cap(*buf)]

	m.buckets[bucket].Put(buf)
}

func (m *memoryPool) bucket(size int) int {
	if size < (1 << shift) {
		return 0
	}
	if size > 1<<(len(m.buckets)+shift-1) {
		return -1
	}

	bucket := bits.Len64(uint64(size)-1) - shift

	return bucket
}
