package toynet

import (
	"bytes"
	"testing"
)

func TestBufferWriteEmpty(t *testing.T) {
	for _, tt := range []struct {
		name string
		buf  buffer
	}{
		{name: "unallocated"},
		{name: "buffered input", buf: buffer{data: []byte("_abc____"), size: 3, readOffset: 1, writeOffset: 4}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.buf
			before := b
			beforeData := string(b.data)
			for _, payload := range [][]byte{nil, {}} {
				if err := b.Write(payload); err != nil {
					t.Errorf("empty Write returned an error: %v", err)
				}
				if b.size != before.size || b.readOffset != before.readOffset || b.writeOffset != before.writeOffset || string(b.data) != beforeData || len(b.data) != len(before.data) || cap(b.data) != cap(before.data) {
					t.Fatal("empty Write changed buffer state")
				}
				if len(b.data) > 0 && &b.data[0] != &before.data[0] {
					t.Fatal("empty Write replaced backing storage")
				}
			}
		})
	}
}

func TestBufferWrite(t *testing.T) {
	for _, tt := range []struct {
		name    string
		initial string
		read    int
		write   int
		size    int
		payload string
		want    string
		grow    bool
	}{
		{name: "existing allocation is sufficient", initial: "_ab_____", read: 1, write: 3, size: 2, payload: "cd", want: "abcd"},
		{name: "allocation grows", initial: string(bytes.Repeat([]byte{'a'}, 256)), size: 256, payload: "bc", want: string(bytes.Repeat([]byte{'a'}, 256)) + "bc", grow: true},
		// Writable segments for these cases are three bytes at the end, then two at the start.
		{name: "less than left writable segment", initial: "__abc___", read: 2, write: 5, size: 3, payload: "de", want: "abcde"},
		{name: "exact left writable segment", initial: "__abc___", read: 2, write: 5, size: 3, payload: "def", want: "abcdef"},
		{name: "left and partial right writable segment", initial: "__abc___", read: 2, write: 5, size: 3, payload: "defg", want: "abcdefg"},
		{name: "both writable segments full", initial: "__abc___", read: 2, write: 5, size: 3, payload: "defgh", want: "abcdefgh"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := newMemoryPool(1024)
			if err != nil {
				t.Fatal(err)
			}
			b := buffer{pool: pool, data: []byte(tt.initial), size: tt.size, readOffset: tt.read, writeOffset: tt.write}
			before := &b.data[0]
			beforeCap := cap(b.data)
			if err := b.Write([]byte(tt.payload)); err != nil {
				t.Fatalf("Write returned an error: %v", err)
			}
			if tt.grow {
				if &b.data[0] == before || cap(b.data) <= beforeCap {
					t.Error("expected a larger backing allocation")
				}
			} else if &b.data[0] != before || cap(b.data) != beforeCap {
				t.Error("Write replaced sufficient backing storage")
			}
			left, right := b.Readable()
			if got := string(left) + string(right); got != tt.want {
				t.Errorf("readable input: want %q, got %q", tt.want, got)
			}
			if b.size != len(tt.want) {
				t.Errorf("size: want %d, got %d", len(tt.want), b.size)
			}
			if got, want := b.available(), cap(b.data)-len(tt.want); got != want {
				t.Errorf("writable space: want %d, got %d", want, got)
			}
		})
	}
}
