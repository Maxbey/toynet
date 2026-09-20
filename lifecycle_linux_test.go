//go:build linux

package toynet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type lifecycleHandler func(Connection) error

func (h lifecycleHandler) OnReadable(c Connection) error { return h(c) }

// Exercise the real event loop with connected stream sockets. No port selection
// or sleeps are needed to arrange data and FIN before the loop starts.
func lifecycleFixture(t *testing.T, h Handler) (*server, epoll, *linuxConnection, int) {
	t.Helper()
	s, err := newServer(Config{MaxBuffer: 1 << 20}, h)
	if err != nil {
		t.Fatal(err)
	}
	p, err := newEpoll()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.close(); err != nil {
			t.Fatalf("closing epoll: %v", err)
		}
	})
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	c := newLinuxConnection(fds[0], make([]byte, 64<<10), s.pool)
	s.connections[c.fd] = connectionRegistration{conn: c, events: unix.EPOLLIN}
	if err := p.add(c.fd, unix.EPOLLIN); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var cleanupErr error
		if err := unix.Close(fds[1]); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("closing peer socket: %w", err))
		}
		// Only clean up entries still owned by the server; avoid closing reused fds.
		for fd, reg := range s.connections {
			if err := unix.Close(fd); err != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("closing server socket %d: %w", fd, err))
			}
			if err := reg.conn.state.close(); err != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("releasing server connection %d: %w", fd, err))
			}
			delete(s.connections, fd)
		}
		if cleanupErr != nil {
			t.Fatal(cleanupErr)
		}
	})
	return s, p, c, fds[1]
}

func runLifecycleLoop(t *testing.T, s *server, p epoll) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() { runErr = s.loop(ctx, p, -1); close(done) }()
	stop := func() {
		cancel()
		select {
		case <-done:
			if runErr != nil {
				t.Errorf("event loop: %v", runErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("event loop did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}

func readLifecycleEOF(t *testing.T, fd int) []byte {
	t.Helper()
	var got []byte
	buf := make([]byte, 8192)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n, err := unix.Read(fd, buf)
		if n > 0 {
			got = append(got, buf[:n]...)
		}
		if err == nil && n == 0 {
			return got
		}
		if err != nil && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			t.Fatalf("reading until EOF: %v", err)
		}
		if errors.Is(err, unix.EAGAIN) {
			_, err := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, 20)
			if err != nil && !errors.Is(err, unix.EINTR) {
				t.Fatal(err)
			}
		}
	}
	t.Fatalf("expected EOF; received %d bytes before timeout", len(got))
	return nil
}

func assertLifecycleClosed(t *testing.T, s *server, c *linuxConnection) {
	t.Helper()
	if _, exists := s.connections[c.fd]; exists {
		t.Error("closed connection remains registered in server map")
	}
	if _, err := unix.FcntlInt(uintptr(c.fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Errorf("socket still open: %v", err)
	}
	if c.state.inputBuf.data != nil || c.state.outputBuf.data != nil {
		t.Error("connection ring storage was not released")
	}
}

func TestSocketReadReportsDataBeforeEOF(t *testing.T) {
	reader, writer := invariantSocketPair(t)
	if _, err := unix.Write(writer, []byte("final request")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Shutdown(writer, unix.SHUT_WR); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, err := socketRead(reader, buf)
	if err != nil {
		t.Fatalf("reading final request: %v", err)
	}
	if got := string(buf[:n]); got != "final request" {
		t.Errorf("want final request, got %q (n=%d)", got, n)
	}

	n, err = socketRead(reader, buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF after final request, got n=%d err=%v", n, err)
	}
	if n != 0 {
		t.Fatalf("want no bytes with EOF, got %d", n)
	}
}

func TestLifecycleHalfClosePreservesFinalRequest(t *testing.T) {
	s, p, c, peer := lifecycleFixture(t, echoTestHandler{})
	want := []byte("echo before closing")
	if _, err := unix.Write(peer, want); err != nil {
		t.Fatal(err)
	}
	if err := unix.Shutdown(peer, unix.SHUT_WR); err != nil {
		t.Fatal(err)
	}
	stop := runLifecycleLoop(t, s, p)
	got := readLifecycleEOF(t, peer)
	stop() // Synchronize before inspecting server-owned state.
	if !bytes.Equal(got, want) {
		t.Errorf("final echo: want %q, got %q", want, got)
	}
	assertLifecycleClosed(t, s, c)
}

func TestLifecycleEOFDrainsBufferedOutput(t *testing.T) {
	s, p, c, peer := lifecycleFixture(t, lifecycleHandler(func(Connection) error { return nil }))
	if err := unix.SetsockoptInt(c.fd, unix.SOL_SOCKET, unix.SO_SNDBUF, 4096); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("pending-response:"), 16384)
	if err := c.state.outputBuf.Write(want); err != nil {
		t.Fatal(err)
	}
	if err := unix.Shutdown(peer, unix.SHUT_WR); err != nil {
		t.Fatal(err)
	}
	stop := runLifecycleLoop(t, s, p)
	got := readLifecycleEOF(t, peer)
	stop()
	if !bytes.Equal(got, want) {
		t.Errorf("response not drained intact: want %d bytes, got %d", len(want), len(got))
	}
	assertLifecycleClosed(t, s, c)
}

func TestLifecycleEmptyEOFCleansConnection(t *testing.T) {
	s, p, c, peer := lifecycleFixture(t, lifecycleHandler(func(Connection) error { return nil }))
	// Retained allocation with no unread bytes should also be released.
	if err := c.state.inputBuf.Write([]byte("used")); err != nil {
		t.Fatal(err)
	}
	if err := c.state.inputBuf.AdvanceRead(4); err != nil {
		t.Fatal(err)
	}
	if err := unix.Shutdown(peer, unix.SHUT_WR); err != nil {
		t.Fatal(err)
	}
	stop := runLifecycleLoop(t, s, p)
	if got := readLifecycleEOF(t, peer); len(got) != 0 {
		t.Errorf("unexpected response %q", got)
	}
	stop()
	assertLifecycleClosed(t, s, c)
}

func TestLifecycleHandlerErrorClosesConnection(t *testing.T) {
	s, p, c, peer := lifecycleFixture(t, lifecycleHandler(func(Connection) error { return errors.New("rejected request") }))
	if _, err := unix.Write(peer, []byte("bad request")); err != nil {
		t.Fatal(err)
	}
	stop := runLifecycleLoop(t, s, p)
	readLifecycleEOF(t, peer)
	stop()
	assertLifecycleClosed(t, s, c)
}
