//go:build linux

package toynet

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"golang.org/x/sys/unix"
)

func invariantSocketPair(t *testing.T) (int, int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var closeErr error
		for _, fd := range fds {
			if err := unix.Close(fd); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("closing socket %d: %w", fd, err))
			}
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	})
	return fds[0], fds[1]
}

func drainInvariantSocket(t *testing.T, fd int) []byte {
	t.Helper()
	var out []byte
	buf := make([]byte, 8192)
	for calls := 0; calls < 1024; calls++ {
		n, err := unix.Read(fd, buf)
		if n > 0 {
			out = append(out, buf[:n]...)
		}
		if errors.Is(err, unix.EAGAIN) {
			return out
		}
		if err != nil || n == 0 {
			t.Fatalf("unexpected read result: n=%d err=%v", n, err)
		}
	}
	t.Fatal("drain exceeded bounded read count")
	return nil
}

func TestSocketReadPreservesUnreadKernelSuffix(t *testing.T) {
	reader, writer := invariantSocketPair(t)
	if n, err := unix.Write(writer, []byte("abcdefgh")); err != nil || n != 8 {
		t.Fatalf("seed write: n=%d err=%v", n, err)
	}
	for _, want := range []string{"abc", "def", "gh", ""} {
		buf := bytes.Repeat([]byte{'?'}, 3)
		n, err := socketRead(reader, buf)
		if err != nil {
			t.Fatal(err)
		}
		if n < 0 || n > len(buf) {
			t.Fatalf("invalid received count %d", n)
		}
		if got := string(buf[:n]); got != want {
			t.Fatalf("want %q, got %q", want, got)
		}
		if !bytes.Equal(buf[n:], bytes.Repeat([]byte{'?'}, len(buf)-n)) {
			t.Fatal("read changed bytes beyond received prefix")
		}
	}
}

func TestWriteConnPreservesOutputUnderBackpressure(t *testing.T) {
	writer, reader := invariantSocketPair(t)
	if err := unix.SetsockoptInt(writer, unix.SOL_SOCKET, unix.SO_SNDBUF, 4096); err != nil {
		t.Fatal(err)
	}
	pool, err := newMemoryPool(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	c := newLinuxConnection(writer, make([]byte, 64<<10), pool)
	t.Cleanup(func() {
		if err := c.state.close(); err != nil {
			t.Fatalf("releasing connection buffers: %v", err)
		}
	})
	// Rotate a full ring so flushing must traverse both readable segments.
	payload := make([]byte, 128<<10)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	if err := c.state.outputBuf.Write(payload); err != nil {
		t.Fatal(err)
	}
	const rotation = 17003
	if err := c.state.outputBuf.AdvanceRead(rotation); err != nil {
		t.Fatal(err)
	}
	if err := c.state.outputBuf.Write(payload[:rotation]); err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte(nil), payload[rotation:]...), payload[:rotation]...)
	s := &server{connections: map[int]connectionRegistration{writer: {conn: c, events: unix.EPOLLIN}}}
	if err := s.writeConn(c); err != nil {
		t.Fatal(err)
	}
	if c.state.outputBuf.Size() == 0 {
		t.Fatal("expected pending output with a small send buffer and idle reader")
	}
	// No reader progress: EAGAIN must preserve the pending output.
	before := c.state.outputBuf.Size()
	if err := s.writeConn(c); err != nil {
		t.Fatal(err)
	}
	if c.state.outputBuf.Size() != before {
		t.Fatal("blocked flush changed buffered byte count")
	}
	var got []byte
	for attempts := 0; attempts < 1024; attempts++ {
		got = append(got, drainInvariantSocket(t, reader)...)
		remaining := c.state.outputBuf.Size()
		if len(got)+remaining != len(want) {
			t.Fatalf("byte accounting: received=%d buffered=%d total=%d", len(got), remaining, len(want))
		}
		if remaining == 0 {
			if !bytes.Equal(got, want) {
				t.Fatal("flushed output was lost, duplicated, or reordered")
			}
			return
		}
		if err := s.writeConn(c); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("output did not drain within bounded flush count")
}

func TestLinuxConnectionWriteKeepsQueuedOutputFirst(t *testing.T) {
	writer, reader := invariantSocketPair(t)
	pool, err := newMemoryPool(1024)
	if err != nil {
		t.Fatal(err)
	}
	c := newLinuxConnection(writer, make([]byte, 64), pool)
	t.Cleanup(func() {
		if err := c.state.close(); err != nil {
			t.Fatalf("releasing connection buffers: %v", err)
		}
	})
	if err := c.state.outputBuf.Write([]byte("older-")); err != nil {
		t.Fatal(err)
	}
	if err := c.Write([]byte("newer")); err != nil {
		t.Fatal(err)
	}
	s := &server{connections: map[int]connectionRegistration{writer: {conn: c, events: unix.EPOLLIN}}}
	if err := s.writeConn(c); err != nil {
		t.Fatal(err)
	}
	if got := string(drainInvariantSocket(t, reader)); got != "older-newer" {
		t.Fatalf("output order: want %q, got %q", "older-newer", got)
	}
}

func TestInputLeftoversSurviveSharedScratchReuse(t *testing.T) {
	pool, err := newMemoryPool(1024)
	if err != nil {
		t.Fatal(err)
	}
	shared := make([]byte, 64)
	view := make([]byte, 64)
	first := newConnection(view, pool)
	second := newConnection(view, pool)
	t.Cleanup(func() {
		if err := first.close(); err != nil {
			t.Fatalf("releasing first connection buffers: %v", err)
		}
	})
	t.Cleanup(func() {
		if err := second.close(); err != nil {
			t.Fatalf("releasing second connection buffers: %v", err)
		}
	})
	copy(shared, "donepartial")
	first.setInputScratch(shared[:11])
	if err := first.Ack(4); err != nil {
		t.Fatal(err)
	}
	if err := first.ringifyInputScratch(); err != nil {
		t.Fatal(err)
	}
	first.setInputScratch(shared[:0]) // Same callback cleanup as the event loop.
	copy(shared, "other")
	second.setInputScratch(shared[:5])
	if _, err := second.PeekAll(); err != nil {
		t.Fatal(err)
	}
	copy(shared, "-rest")
	first.setInputScratch(shared[:5])
	got, err := first.PeekAll()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "partial-rest" {
		t.Fatalf("resumed input: want %q, got %q", "partial-rest", got)
	}
}
