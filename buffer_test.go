package toynet

import (
	"bytes"
	"testing"
)

func TestBufferReadable(t *testing.T) {
	tests := []struct {
		name       string
		buffer     buffer
		wantFirst  []byte
		wantSecond []byte
	}{
		{name: "zero value"},
		{
			name:   "allocated but empty",
			buffer: buffer{data: make([]byte, 8)},
		},
		{
			name: "contiguous",
			buffer: buffer{
				data:        []byte("_abc____"),
				size:        3,
				readOffset:  1,
				writeOffset: 4,
			},
			wantFirst: []byte("abc"),
		},
		{
			name: "wrapped",
			buffer: buffer{
				data:        []byte("cde___ab"),
				size:        5,
				readOffset:  6,
				writeOffset: 3,
			},
			wantFirst:  []byte("ab"),
			wantSecond: []byte("cde"),
		},
		{
			name: "full at zero offset",
			buffer: buffer{
				data: []byte("abcd"),
				size: 4,
			},
			wantFirst: []byte("abcd"),
		},
		{
			name: "full at wrapped offset",
			buffer: buffer{
				data:        []byte("defabc"),
				size:        6,
				readOffset:  3,
				writeOffset: 3,
			},
			wantFirst:  []byte("abc"),
			wantSecond: []byte("def"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first, second := tt.buffer.Readable()

			if !bytes.Equal(first, tt.wantFirst) {
				t.Errorf("first readable slice = %q, want %q", first, tt.wantFirst)
			}
			if !bytes.Equal(second, tt.wantSecond) {
				t.Errorf("second readable slice = %q, want %q", second, tt.wantSecond)
			}
			if got := len(first) + len(second); got != tt.buffer.size {
				t.Errorf("total readable length = %d, want %d", got, tt.buffer.size)
			}
		})
	}
}

func TestBufferAvailable(t *testing.T) {
	tests := []struct {
		name   string
		buffer buffer
		want   int
	}{
		{name: "zero value"},
		{
			name:   "allocated but empty at zero offset",
			buffer: buffer{data: make([]byte, 8)},
			want:   8,
		},
		{
			name: "allocated but empty at equal nonzero offsets",
			buffer: buffer{
				data:        make([]byte, 8),
				readOffset:  3,
				writeOffset: 3,
			},
			want: 8,
		},
		{
			name: "write offset ahead of read offset",
			buffer: buffer{
				data:        make([]byte, 8),
				size:        3,
				readOffset:  1,
				writeOffset: 4,
			},
			want: 5,
		},
		{
			name: "read offset ahead of write offset",
			buffer: buffer{
				data:        make([]byte, 8),
				size:        5,
				readOffset:  6,
				writeOffset: 3,
			},
			want: 3,
		},
		{
			name: "full at zero offset",
			buffer: buffer{
				data: make([]byte, 8),
				size: 8,
			},
		},
		{
			name: "full at equal nonzero offsets",
			buffer: buffer{
				data:        make([]byte, 8),
				size:        8,
				readOffset:  3,
				writeOffset: 3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.buffer.available(); got != tt.want {
				t.Errorf("available() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBufferWritable(t *testing.T) {
	tests := []struct {
		name       string
		buffer     buffer
		wantFirst  []byte
		wantSecond []byte
	}{
		{name: "zero value"},
		{
			name:       "allocated but empty",
			buffer:     buffer{data: []byte("________")},
			wantFirst:  []byte("________"),
			wantSecond: nil,
		},
		{
			name: "writable space wraps",
			buffer: buffer{
				data:        []byte("_abc____"),
				size:        3,
				readOffset:  1,
				writeOffset: 4,
			},
			wantFirst:  []byte("____"),
			wantSecond: []byte("_"),
		},
		{
			name: "writable space is contiguous",
			buffer: buffer{
				data:        []byte("cde___ab"),
				size:        5,
				readOffset:  6,
				writeOffset: 3,
			},
			wantFirst: []byte("___"),
		},
		{
			name: "full at zero offset",
			buffer: buffer{
				data: []byte("abcd"),
				size: 4,
			},
		},
		{
			name: "full at wrapped offset",
			buffer: buffer{
				data:        []byte("defabc"),
				size:        6,
				readOffset:  3,
				writeOffset: 3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first, second := tt.buffer.writable()

			if !bytes.Equal(first, tt.wantFirst) {
				t.Errorf("first writable slice = %q, want %q", first, tt.wantFirst)
			}
			if !bytes.Equal(second, tt.wantSecond) {
				t.Errorf("second writable slice = %q, want %q", second, tt.wantSecond)
			}
			if got := len(first) + len(second); got != tt.buffer.available() {
				t.Errorf("total writable length = %d, want %d", got, tt.buffer.available())
			}
		})
	}
}

func TestBufferAlloc(t *testing.T) {
	pool, err := newMemoryPool(2 << 20)
	if err != nil {
		t.Fatalf("newMemoryPool returned an error: %v", err)
	}

	t.Run("negative request", func(t *testing.T) {
		b := buffer{pool: pool, data: make([]byte, 256)}
		if err := b.alloc(-1); err == nil {
			t.Fatal("Alloc accepted a negative request")
		}
	})

	t.Run("empty buffer", func(t *testing.T) {
		b := buffer{pool: pool}
		if err := b.alloc(100); err != nil {
			t.Fatalf("Alloc returned an error: %v", err)
		}
		if len(b.data) != 256 || cap(b.data) != 256 {
			t.Fatalf("allocated buffer length and capacity = %d, %d; want 256, 256", len(b.data), cap(b.data))
		}
		if b.size != 0 || b.readOffset != 0 || b.writeOffset != 0 {
			t.Fatalf("empty buffer state changed to size=%d readOffset=%d writeOffset=%d", b.size, b.readOffset, b.writeOffset)
		}
	})

	t.Run("allocated empty buffer grows without becoming full", func(t *testing.T) {
		b := buffer{
			pool:        pool,
			data:        make([]byte, 256),
			readOffset:  100,
			writeOffset: 100,
		}

		if err := b.alloc(300); err != nil {
			t.Fatalf("Alloc returned an error: %v", err)
		}
		if len(b.data) != 512 || cap(b.data) != 512 {
			t.Fatalf("grown buffer length and capacity = %d, %d; want 512, 512", len(b.data), cap(b.data))
		}
		if b.size != 0 {
			t.Fatalf("size after growth = %d, want 0", b.size)
		}
		if b.readOffset != 100 || b.writeOffset != 100 {
			t.Fatalf("offsets after growth = read %d, write %d; want read 100, write 100", b.readOffset, b.writeOffset)
		}
		if first, second := b.Readable(); first != nil || second != nil {
			t.Fatalf("empty buffer returned readable slices of lengths %d and %d", len(first), len(second))
		}
		first, second := b.writable()
		if got := len(first) + len(second); got != 512 {
			t.Fatalf("total writable length after growth = %d, want 512", got)
		}
	})

	t.Run("enough space is a no-op", func(t *testing.T) {
		b := buffer{
			pool:        pool,
			data:        make([]byte, 256),
			size:        3,
			readOffset:  1,
			writeOffset: 4,
		}
		before := &b.data[0]

		if err := b.alloc(253); err != nil {
			t.Fatalf("Alloc returned an error: %v", err)
		}
		if &b.data[0] != before {
			t.Fatal("Alloc replaced a buffer with sufficient writable space")
		}
	})

	t.Run("contiguous readable data survives growth", func(t *testing.T) {
		data := make([]byte, 256)
		for i := 20; i < 220; i++ {
			data[i] = byte(i)
		}
		want := append([]byte(nil), data[20:220]...)
		b := buffer{
			pool:        pool,
			data:        data,
			size:        200,
			readOffset:  20,
			writeOffset: 220,
		}

		if err := b.alloc(100); err != nil {
			t.Fatalf("Alloc returned an error: %v", err)
		}
		if cap(b.data) != 512 {
			t.Fatalf("grown buffer capacity = %d, want 512", cap(b.data))
		}
		if got := readableData(&b); !bytes.Equal(got, want) {
			t.Fatalf("readable data after growth = %v, want %v", got, want)
		}
		if b.available() < 100 {
			t.Fatalf("available space after growth = %d, want at least 100", b.available())
		}
	})

	t.Run("wrapped readable data survives growth", func(t *testing.T) {
		data := make([]byte, 256)
		copy(data[200:], bytes.Repeat([]byte{'a'}, 56))
		copy(data[:100], bytes.Repeat([]byte{'b'}, 100))
		want := append(bytes.Repeat([]byte{'a'}, 56), bytes.Repeat([]byte{'b'}, 100)...)
		b := buffer{
			pool:        pool,
			data:        data,
			size:        156,
			readOffset:  200,
			writeOffset: 100,
		}

		if err := b.alloc(101); err != nil {
			t.Fatalf("Alloc returned an error: %v", err)
		}
		if cap(b.data) != 512 {
			t.Fatalf("grown buffer capacity = %d, want 512", cap(b.data))
		}
		if got := readableData(&b); !bytes.Equal(got, want) {
			t.Fatalf("readable data after growth has length %d, want %d", len(got), len(want))
		}
		if b.available() < 101 {
			t.Fatalf("available space after growth = %d, want at least 101", b.available())
		}
	})

	t.Run("full buffer survives growth", func(t *testing.T) {
		data := make([]byte, 256)
		for i := range data {
			data[i] = byte(i)
		}
		want := append(append([]byte(nil), data[100:]...), data[:100]...)
		b := buffer{
			pool:        pool,
			data:        data,
			size:        256,
			readOffset:  100,
			writeOffset: 100,
		}

		if err := b.alloc(1); err != nil {
			t.Fatalf("Alloc returned an error: %v", err)
		}
		if cap(b.data) != 512 {
			t.Fatalf("grown buffer capacity = %d, want 512", cap(b.data))
		}
		if b.size != 256 {
			t.Fatalf("size after growth = %d, want 256", b.size)
		}
		if b.readOffset != 0 || b.writeOffset != 256 {
			t.Fatalf("offsets after growth = read %d, write %d; want read 0, write 256", b.readOffset, b.writeOffset)
		}
		if got := readableData(&b); !bytes.Equal(got, want) {
			t.Fatalf("readable data after growth differs from original logical order")
		}
		if b.available() != 256 {
			t.Fatalf("available space after growth = %d, want 256", b.available())
		}
	})

	t.Run("request exceeding maximum leaves buffer unchanged", func(t *testing.T) {
		data := make([]byte, 2<<20)
		b := buffer{pool: pool, data: data, size: len(data)}
		before := &b.data[0]

		if err := b.alloc(1); err == nil {
			t.Fatal("Alloc accepted a request exceeding the pool maximum")
		}
		if &b.data[0] != before {
			t.Fatal("Alloc replaced the buffer after a failed request")
		}
	})
}

func TestBufferAdvanceRead(t *testing.T) {
	tests := []struct {
		name           string
		buffer         buffer
		n              int
		wantErr        bool
		wantReadOffset int
		wantSize       int
	}{
		{name: "negative", buffer: buffer{data: make([]byte, 8), size: 3, readOffset: 1, writeOffset: 4}, n: -1, wantErr: true, wantReadOffset: 1, wantSize: 3},
		{name: "zero", buffer: buffer{data: make([]byte, 8), size: 3, readOffset: 1, writeOffset: 4}, n: 0, wantReadOffset: 1, wantSize: 3},
		{name: "zero value", n: 1, wantErr: true},
		{name: "ordinary", buffer: buffer{data: make([]byte, 8), size: 3, readOffset: 1, writeOffset: 4}, n: 2, wantReadOffset: 3, wantSize: 1},
		{name: "wraps", buffer: buffer{data: make([]byte, 8), size: 5, readOffset: 6, writeOffset: 3}, n: 3, wantReadOffset: 1, wantSize: 2},
		{name: "exact limit becomes empty", buffer: buffer{data: make([]byte, 8), size: 3, readOffset: 1, writeOffset: 4}, n: 3, wantReadOffset: 4, wantSize: 0},
		{name: "full wrapped buffer becomes empty", buffer: buffer{data: make([]byte, 8), size: 8, readOffset: 3, writeOffset: 3}, n: 8, wantReadOffset: 3, wantSize: 0},
		{name: "beyond limit", buffer: buffer{data: make([]byte, 8), size: 3, readOffset: 1, writeOffset: 4}, n: 4, wantErr: true, wantReadOffset: 1, wantSize: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.buffer.AdvanceRead(tt.n)
			if tt.wantErr && err == nil {
				t.Fatal("AdvanceRead returned no error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("AdvanceRead returned an error: %v", err)
			}
			if tt.buffer.readOffset != tt.wantReadOffset {
				t.Errorf("read offset = %d, want %d", tt.buffer.readOffset, tt.wantReadOffset)
			}
			if tt.buffer.size != tt.wantSize {
				t.Errorf("size = %d, want %d", tt.buffer.size, tt.wantSize)
			}
		})
	}
}

func TestBufferAdvanceWrite(t *testing.T) {
	tests := []struct {
		name            string
		buffer          buffer
		n               int
		wantErr         bool
		wantWriteOffset int
		wantSize        int
	}{
		{name: "negative", buffer: buffer{data: make([]byte, 8)}, n: -1, wantErr: true},
		{name: "zero", buffer: buffer{data: make([]byte, 8)}, n: 0},
		{name: "zero value", n: 1, wantErr: true},
		{name: "ordinary", buffer: buffer{data: make([]byte, 8)}, n: 3, wantWriteOffset: 3, wantSize: 3},
		{name: "wraps", buffer: buffer{data: make([]byte, 8), size: 3, readOffset: 3, writeOffset: 6}, n: 3, wantWriteOffset: 1, wantSize: 6},
		{name: "exact limit becomes full", buffer: buffer{data: make([]byte, 8), size: 5, readOffset: 6, writeOffset: 3}, n: 3, wantWriteOffset: 6, wantSize: 8},
		{name: "beyond limit", buffer: buffer{data: make([]byte, 8), size: 5, readOffset: 6, writeOffset: 3}, n: 4, wantErr: true, wantWriteOffset: 3, wantSize: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.buffer.advanceWrite(tt.n)
			if tt.wantErr && err == nil {
				t.Fatal("AdvanceWrite returned no error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("AdvanceWrite returned an error: %v", err)
			}
			if tt.buffer.writeOffset != tt.wantWriteOffset {
				t.Errorf("write offset = %d, want %d", tt.buffer.writeOffset, tt.wantWriteOffset)
			}
			if tt.buffer.size != tt.wantSize {
				t.Errorf("size = %d, want %d", tt.buffer.size, tt.wantSize)
			}
		})
	}
}

func TestBufferFree(t *testing.T) {
	pool, err := newMemoryPool(2 << 20)
	if err != nil {
		t.Fatalf("newMemoryPool returned an error: %v", err)
	}

	t.Run("unallocated buffer", func(t *testing.T) {
		var b buffer
		if err := b.Free(); err != nil {
			t.Fatalf("Free returned an error: %v", err)
		}
	})

	t.Run("releases allocation and resets state", func(t *testing.T) {
		b := buffer{
			pool:        pool,
			data:        make([]byte, 256),
			size:        156,
			readOffset:  200,
			writeOffset: 100,
		}

		if err := b.Free(); err != nil {
			t.Fatalf("Free returned an error: %v", err)
		}
		if b.data != nil {
			t.Fatalf("data after Free has length %d, want nil", len(b.data))
		}
		if b.size != 0 || b.readOffset != 0 || b.writeOffset != 0 {
			t.Fatalf("state after Free = size %d, read %d, write %d; want all zero", b.size, b.readOffset, b.writeOffset)
		}

		if err := b.Free(); err != nil {
			t.Fatalf("second Free returned an error: %v", err)
		}
	})

	t.Run("release error is returned after invalidation", func(t *testing.T) {
		b := buffer{
			pool:        pool,
			data:        make([]byte, 300),
			size:        100,
			readOffset:  20,
			writeOffset: 120,
		}

		if err := b.Free(); err == nil {
			t.Fatal("Free returned no error for a non-class buffer")
		}
		if b.data != nil || b.size != 0 || b.readOffset != 0 || b.writeOffset != 0 {
			t.Fatalf("buffer was not invalidated after release error")
		}
	})
}

func readableData(b *buffer) []byte {
	first, second := b.Readable()
	data := make([]byte, 0, len(first)+len(second))
	data = append(data, first...)
	return append(data, second...)
}
