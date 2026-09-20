package toynet

import (
	"bytes"
	"testing"
)

func TestConnectionAckRingAndScratch(t *testing.T) {
	tests := []struct {
		name        string
		n           int
		wantErr     bool
		wantRing    string
		wantScratch string
		wantRead    int
	}{
		{name: "more than all buffered input", n: 12, wantErr: true, wantRing: "abcdefg", wantScratch: "hijk", wantRead: 7},
		{name: "less than ring size", n: 5, wantRing: "fg", wantScratch: "hijk", wantRead: 2},
		{name: "exact ring size", n: 7, wantScratch: "hijk", wantRead: 4},
		{name: "ring and part of scratch", n: 9, wantScratch: "jk", wantRead: 4},
		{name: "ring and all scratch", n: 11, wantRead: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Logical input: wrapped ring "abcdefg", followed by scratch "hijk".
			c := &connection{
				inputBuf: buffer{
					data:        []byte("defg___abc"),
					size:        7,
					readOffset:  7,
					writeOffset: 4,
				},
				inputScratch: []byte("hijk"),
			}

			err := c.Ack(tt.n)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Ack(%d) error = %v, want error: %t", tt.n, err, tt.wantErr)
			}
			left, right := c.inputBuf.Readable()
			if got := string(left) + string(right); got != tt.wantRing {
				t.Errorf("remaining ring: want %q, got %q", tt.wantRing, got)
			}
			if got := string(c.inputScratch); got != tt.wantScratch {
				t.Errorf("remaining scratch: want %q, got %q", tt.wantScratch, got)
			}
			if c.inputBuf.size != len(tt.wantRing) || c.inputBuf.readOffset != tt.wantRead {
				t.Errorf("ring state: want size=%d read=%d, got size=%d read=%d", len(tt.wantRing), tt.wantRead, c.inputBuf.size, c.inputBuf.readOffset)
			}
			if c.inputBuf.writeOffset != 4 || string(c.inputBuf.data) != "defg___abc" {
				t.Error("Ack changed the ring write position or underlying bytes")
			}
		})
	}
}

func TestConnectionAckAllScratchWithEmptyRing(t *testing.T) {
	c := &connection{inputScratch: []byte("hello")}

	if err := c.Ack(len(c.inputScratch)); err != nil {
		t.Fatalf("Ack(5) returned an error: %v", err)
	}
	if len(c.inputScratch) != 0 {
		t.Errorf("remaining scratch: want empty, got %q", c.inputScratch)
	}
	if c.inputBuf.size != 0 || c.inputBuf.readOffset != 0 || c.inputBuf.writeOffset != 0 || c.inputBuf.data != nil {
		t.Error("acknowledging scratch changed the empty ring")
	}
}

func TestConnectionAckZero(t *testing.T) {
	for _, tt := range []struct {
		name    string
		ring    string
		scratch string
	}{
		{name: "empty input"},
		{name: "ring only", ring: "abc"},
		{name: "scratch only", scratch: "def"},
		{name: "ring and scratch", ring: "abc", scratch: "def"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &connection{
				inputBuf: buffer{
					data:        []byte("_" + tt.ring + "_"),
					size:        len(tt.ring),
					readOffset:  1,
					writeOffset: 1 + len(tt.ring),
				},
				inputScratch: []byte(tt.scratch),
			}
			if tt.ring == "" {
				c.inputBuf = buffer{}
			}
			before := c.inputBuf
			beforeData := string(before.data)
			if err := c.Ack(0); err == nil {
				t.Error("Ack(0) should reject a zero count")
			}
			if c.inputBuf.size != before.size || c.inputBuf.readOffset != before.readOffset || c.inputBuf.writeOffset != before.writeOffset || string(c.inputBuf.data) != beforeData {
				t.Error("Ack(0) changed ring input")
			}
			if got := string(c.inputScratch); got != tt.scratch {
				t.Errorf("scratch after Ack(0): want %q, got %q", tt.scratch, got)
			}
		})
	}
}

func TestConnectionPeekViewGrowth(t *testing.T) {
	for _, tt := range []struct {
		name string
		max  int
		fail bool
	}{
		{name: "grows beyond shared view", max: 256 << 10},
		{name: "pool limit exceeded", max: 64 << 10, fail: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := newMemoryPool(tt.max)
			if err != nil {
				t.Fatal(err)
			}
			shared := bytes.Repeat([]byte{'?'}, 64<<10)
			scratch := bytes.Repeat([]byte{'x'}, 64<<10)
			c := &connection{
				pool:         pool,
				inputBuf:     buffer{data: []byte("bc_a"), size: 3, readOffset: 3, writeOffset: 2},
				inputScratch: scratch,
				viewScratch:  shared,
			}
			want := append([]byte("abc"), scratch...)
			got, err := c.Peek(len(want))
			if tt.fail {
				if err == nil {
					t.Fatal("expected pool acquisition error")
				}
				if len(got) != 0 {
					t.Errorf("want no result on error, got %d bytes", len(got))
				}
				if &c.viewScratch[0] != &shared[0] {
					t.Error("failed growth replaced the existing view buffer")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, want) {
					t.Errorf("want %d matching bytes, got %d bytes with mismatched content or length", len(want), len(got))
				}
				if &c.viewScratch[0] == &shared[0] {
					t.Error("expected a separate allocation after growth")
				}
			}
			if !bytes.Equal(shared, bytes.Repeat([]byte{'?'}, len(shared))) {
				t.Error("oversized peek modified shared view storage")
			}
			if c.inputBuf.readOffset != 3 || c.inputBuf.writeOffset != 2 || c.inputBuf.size != 3 || string(c.inputBuf.data) != "bc_a" {
				t.Error("peek changed ring input")
			}
			if len(c.inputScratch) != len(scratch) || &c.inputScratch[0] != &scratch[0] || !bytes.Equal(scratch, want[3:]) {
				t.Error("peek changed scratch input")
			}
		})
	}
}

func TestConnectionPeekRepeatedWithoutConsumption(t *testing.T) {
	c := &connection{
		inputBuf:     buffer{data: []byte("defg___abc"), size: 7, readOffset: 7, writeOffset: 4},
		inputScratch: []byte("hijk"),
		viewScratch:  make([]byte, 64),
	}
	scratchStart := &c.inputScratch[0]
	const input = "abcdefghijk"
	for _, n := range []int{2, 2, 5, 5, 11, 3, 9, 11, 15} {
		got, err := c.Peek(n)
		if err != nil {
			t.Fatalf("Peek(%d): %v", n, err)
		}
		want := input[:min(n, len(input))]
		if string(got) != want {
			t.Errorf("Peek(%d): want %q, got %q", n, want, got)
		}
		if c.inputBuf.readOffset != 7 || c.inputBuf.writeOffset != 4 || c.inputBuf.size != 7 || string(c.inputBuf.data) != "defg___abc" {
			t.Fatalf("Peek(%d) changed ring input", n)
		}
		if string(c.inputScratch) != "hijk" || &c.inputScratch[0] != scratchStart {
			t.Fatalf("Peek(%d) changed scratch input", n)
		}
	}
}

func TestConnectionPeekContiguousRingAndScratch(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want string
	}{
		{name: "ring and part of scratch", n: 5, want: "abcde"},
		{name: "ring and all scratch", n: 7, want: "abcdefg"},
		{name: "more than all available input", n: 10, want: "abcdefg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Logical input: contiguous ring "abc", no right segment, scratch "defg".
			c := &connection{
				inputBuf: buffer{
					data:        []byte("_abc____"),
					size:        3,
					readOffset:  1,
					writeOffset: 4,
				},
				inputScratch: []byte("defg"),
				viewScratch:  make([]byte, 64),
			}

			got, err := c.Peek(tt.n)
			if err != nil {
				t.Fatalf("Peek(%d) returned an error: %v", tt.n, err)
			}
			if string(got) != tt.want {
				t.Errorf("Peek(%d): want %q (length %d), got %q (length %d)", tt.n, tt.want, len(tt.want), got, len(got))
			}
		})
	}
}

func TestConnectionPeekEmptyZeroNegativeAndScratchOnly(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		n       int
		want    string
		wantErr bool
	}{
		{name: "empty input", n: 5},
		{name: "empty input and zero n", n: 0},
		{name: "zero n with input", input: "hello", n: 0},
		{name: "negative n with empty input", n: -1, wantErr: true},
		{name: "negative n with input", input: "hello", n: -1, wantErr: true},
		{name: "scratch only partial", input: "hello", n: 2, want: "he"},
		{name: "scratch only exact", input: "hello", n: 5, want: "hello"},
		{name: "scratch only more than available", input: "hello", n: 8, want: "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &connection{
				inputScratch: []byte(tt.input),
				viewScratch:  make([]byte, 64),
			}

			got, err := c.Peek(tt.n)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Peek(%d) error = %v, want error: %t", tt.n, err, tt.wantErr)
			}
			if string(got) != tt.want {
				t.Errorf("Peek(%d): want %q (length %d), got %q (length %d)", tt.n, tt.want, len(tt.want), got, len(got))
			}
		})
	}
}

func TestConnectionPeekWrappedInput(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want string
	}{
		{name: "shorter than left", n: 2, want: "ab"},
		{name: "left and part of right", n: 5, want: "abcde"},
		{name: "left and right", n: 7, want: "abcdefg"},
		{name: "left right and part of scratch", n: 9, want: "abcdefghi"},
		{name: "left right and all scratch", n: 11, want: "abcdefghijk"},
		{name: "more than all available input", n: 15, want: "abcdefghijk"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Logical input: left "abc", right "defg", scratch "hijk".
			c := &connection{
				inputBuf: buffer{
					data:        []byte("defg___abc"),
					size:        7,
					readOffset:  7,
					writeOffset: 4,
				},
				inputScratch: []byte("hijk"),
				viewScratch:  make([]byte, 64),
			}

			got, err := c.Peek(tt.n)
			if err != nil {
				t.Fatalf("Peek(%d) returned an error: %v", tt.n, err)
			}
			if string(got) != tt.want {
				t.Errorf("Peek(%d) = %q (length %d), want %q (length %d)", tt.n, got, len(got), tt.want, len(tt.want))
			}
		})
	}
}
