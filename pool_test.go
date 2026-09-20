package toynet

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

func TestNewMemoryPoolInitializesBuckets(t *testing.T) {
	pool, err := newMemoryPool(2 << 20)
	if err != nil {
		t.Fatalf("newMemoryPool returned an error: %v", err)
	}

	for bucket := range pool.buckets {
		wantSize := 1 << (shift + bucket)
		buf := pool.buckets[bucket].Get().(*[]byte)

		if got := len(*buf); got != wantSize {
			t.Errorf("bucket %d buffer length = %d, want %d", bucket, got, wantSize)
		}
		if got := cap(*buf); got != wantSize {
			t.Errorf("bucket %d buffer capacity = %d, want %d", bucket, got, wantSize)
		}
	}
}

func TestMemoryPoolAcquire(t *testing.T) {
	tests := []struct {
		name     string
		request  int
		wantSize int
		wantErr  bool
	}{
		{name: "zero", request: 0, wantErr: true},
		{name: "one byte", request: 1, wantSize: 1 << shift},
		{name: "two hundred fifty-six bytes", request: 1 << shift, wantSize: 1 << shift},
		{name: "above two hundred fifty-six bytes", request: 1<<shift + 1, wantSize: 1 << (shift + 1)},
		{name: "five hundred twelve bytes", request: 1 << 9, wantSize: 1 << 9},
		{name: "above five hundred twelve bytes", request: 1<<9 + 1, wantSize: 1 << 10},
		{name: "one KiB", request: 1 << 10, wantSize: 1 << 10},
		{name: "above one KiB", request: 1<<10 + 1, wantSize: 2 << 10},
		{name: "two KiB", request: 2 << 10, wantSize: 2 << 10},
		{name: "above two KiB", request: 2<<10 + 1, wantSize: 4 << 10},
		{name: "four KiB", request: 4 << 10, wantSize: 4 << 10},
		{name: "above four KiB", request: 4<<10 + 1, wantSize: 8 << 10},
		{name: "eight KiB", request: 8 << 10, wantSize: 8 << 10},
		{name: "above eight KiB", request: 8<<10 + 1, wantSize: 16 << 10},
		{name: "sixteen KiB", request: 16 << 10, wantSize: 16 << 10},
		{name: "above sixteen KiB", request: 16<<10 + 1, wantSize: 32 << 10},
		{name: "thirty-two KiB", request: 32 << 10, wantSize: 32 << 10},
		{name: "above thirty-two KiB", request: 32<<10 + 1, wantSize: 64 << 10},
		{name: "sixty-four KiB", request: 64 << 10, wantSize: 64 << 10},
		{name: "above sixty-four KiB", request: 64<<10 + 1, wantSize: 128 << 10},
		{name: "one hundred twenty-eight KiB", request: 128 << 10, wantSize: 128 << 10},
		{name: "above one hundred twenty-eight KiB", request: 128<<10 + 1, wantSize: 256 << 10},
		{name: "two hundred fifty-six KiB", request: 256 << 10, wantSize: 256 << 10},
		{name: "above two hundred fifty-six KiB", request: 256<<10 + 1, wantSize: 512 << 10},
		{name: "five hundred twelve KiB", request: 512 << 10, wantSize: 512 << 10},
		{name: "above five hundred twelve KiB", request: 512<<10 + 1, wantSize: 1 << 20},
		{name: "one MiB", request: 1 << 20, wantSize: 1 << 20},
		{name: "above one MiB", request: 1<<20 + 1, wantSize: 2 << 20},
		{name: "two MiB", request: 2 << 20, wantSize: 2 << 20},
		{name: "above maximum buffer size", request: 2<<20 + 1, wantErr: true},
	}

	pool, err := newMemoryPool(2 << 20)
	if err != nil {
		t.Fatalf("newMemoryPool returned an error: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf, err := pool.Acquire(tt.request)
			if tt.wantErr {
				if err == nil {
					t.Fatal("Acquire returned no error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Acquire returned an error: %v", err)
			}
			if buf == nil {
				t.Fatal("Acquire returned a nil pointer")
			}
			if got := len(*buf); got != tt.wantSize {
				t.Errorf("buffer length = %d, want %d", got, tt.wantSize)
			}
			if got := cap(*buf); got != tt.wantSize {
				t.Errorf("buffer capacity = %d, want %d", got, tt.wantSize)
			}
		})
	}
}

func TestMemoryPoolReleaseRejectsInvalidBuffers(t *testing.T) {
	pool, err := newMemoryPool(2 << 20)
	if err != nil {
		t.Fatalf("newMemoryPool returned an error: %v", err)
	}

	tests := []struct {
		name string
		buf  *[]byte
	}{
		{name: "nil pointer"},
		{name: "nil slice", buf: new([]byte)},
		{name: "undersized", buf: newBuffer(1<<9 - 1)},
		{name: "non-class capacity", buf: newBuffer(5 << 10)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := pool.Release(tt.buf); err == nil {
				t.Fatal("Release accepted an invalid buffer")
			}
		})
	}
}

func TestMemoryPoolReleaseAcceptsEveryClass(t *testing.T) {
	pool, err := newMemoryPool(2 << 20)
	if err != nil {
		t.Fatalf("newMemoryPool returned an error: %v", err)
	}

	for bucket := range pool.buckets {
		capacity := 1 << (shift + bucket)
		t.Run(sizeName(capacity), func(t *testing.T) {
			buf := newBuffer(capacity)
			if err := pool.Release(buf); err != nil {
				t.Fatalf("Release returned an error: %v", err)
			}
		})
	}
}

func TestMemoryPoolReleaseAcceptsShortenedClassBuffer(t *testing.T) {
	pool, err := newMemoryPool(2 << 20)
	if err != nil {
		t.Fatalf("newMemoryPool returned an error: %v", err)
	}
	data := make([]byte, 16, 4<<10)
	for i := range data {
		data[i] = 0xff
	}

	if err := pool.Release(&data); err != nil {
		t.Fatalf("Release returned an error: %v", err)
	}
}

func TestMemoryPoolReleaseDropsOversizedBuffer(t *testing.T) {
	pool, err := newMemoryPool(2 << 20)
	if err != nil {
		t.Fatalf("newMemoryPool returned an error: %v", err)
	}
	buf := newBuffer(16<<20 + 1)

	if err := pool.Release(buf); err != nil {
		t.Fatalf("Release returned an error for an oversized buffer: %v", err)
	}
}

func TestMemoryPoolRoundTrip(t *testing.T) {
	pool, err := newMemoryPool(2 << 20)
	if err != nil {
		t.Fatalf("newMemoryPool returned an error: %v", err)
	}

	for bucket := range pool.buckets {
		capacity := 1 << (shift + bucket)
		t.Run(sizeName(capacity), func(t *testing.T) {
			buf, err := pool.Acquire(capacity)
			if err != nil {
				t.Fatalf("Acquire returned an error: %v", err)
			}
			*buf = (*buf)[:capacity/2]

			if err := pool.Release(buf); err != nil {
				t.Fatalf("Release returned an error: %v", err)
			}

			got, err := pool.Acquire(capacity)
			if err != nil {
				t.Fatalf("Acquire returned an error: %v", err)
			}
			if len(*got) != capacity || cap(*got) != capacity {
				t.Fatalf("reallocated buffer length and capacity = %d, %d; want %d, %d", len(*got), cap(*got), capacity, capacity)
			}
		})
	}
}

func TestMemoryPoolConcurrentAcquireRelease(t *testing.T) {
	pool, err := newMemoryPool(2 << 20)
	if err != nil {
		t.Fatalf("newMemoryPool returned an error: %v", err)
	}

	const (
		goroutines = 32
		iterations = 500
	)

	start := make(chan struct{})
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup

	for worker := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			for iteration := range iterations {
				bucket := (worker + iteration) % len(pool.buckets)
				wantSize := 1 << (shift + bucket)
				buf, err := pool.Acquire(wantSize)
				if err != nil {
					errs <- fmt.Errorf("worker %d acquire: %w", worker, err)
					return
				}
				if len(*buf) != wantSize || cap(*buf) != wantSize {
					errs <- fmt.Errorf("worker %d acquired length and capacity %d, %d; want %d, %d", worker, len(*buf), cap(*buf), wantSize, wantSize)
					return
				}

				marker := byte(worker + 1)
				(*buf)[0] = marker
				(*buf)[len(*buf)-1] = marker
				runtime.Gosched()
				if (*buf)[0] != marker || (*buf)[len(*buf)-1] != marker {
					errs <- fmt.Errorf("worker %d buffer changed while acquired", worker)
					return
				}

				if err := pool.Release(buf); err != nil {
					errs <- fmt.Errorf("worker %d release: %w", worker, err)
					return
				}
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

func newBuffer(capacity int) *[]byte {
	buf := make([]byte, capacity)
	return &buf
}

func sizeName(size int) string {
	return fmt.Sprintf("%d_KiB", size>>10)
}
