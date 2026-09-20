//go:build linux

package toynet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type echoTestHandler struct{}

func (echoTestHandler) OnReadable(c Connection) error {
	input, err := c.PeekAll()
	if err != nil || len(input) == 0 {
		return err
	}
	if err := c.Write(input); err != nil {
		return err
	}
	return c.Ack(len(input))
}

func TestServerEcho(t *testing.T) {
	// Reserve a local port briefly because Run does not expose its bound address.
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := reservation.Addr().(*net.TCPAddr).Port
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := newServer(Config{
		Host: "127.0.0.1", Port: port, Protocol: TCP,
		MaxConnections: 16, MaxBuffer: 1 << 20,
	}, echoTestHandler{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() {
		runErr = s.Run(ctx)
		close(done)
	}()
	var client net.Conn
	t.Cleanup(func() {
		cancel()
		var cleanupErr error
		if client != nil {
			if err := client.Close(); err != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("closing echo client: %w", err))
			}
		}
		select {
		case <-done:
			// Temporary test cleanup until the server owns connection shutdown.
			for fd, c := range s.connections {
				if err := unix.Close(fd); err != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("closing server socket %d: %w", fd, err))
				}
				if err := c.conn.state.close(); err != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("releasing server connection %d: %w", fd, err))
				}
			}
			if runErr != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("server stopped: %w", runErr))
			}
		case <-time.After(2 * time.Second):
			cleanupErr = errors.Join(cleanupErr, errors.New("server did not stop after cancellation"))
		}
		if cleanupErr != nil {
			t.Fatal(cleanupErr)
		}
	})

	address := net.JoinHostPort("127.0.0.1", fmt.Sprint(port))
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case <-done:
			t.Fatalf("server exited before connecting: %v", runErr)
		default:
		}
		client, err = net.DialTimeout("tcp4", address, 100*time.Millisecond)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("connecting to echo server: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := client.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"hello toynet", "another echo on the same connection"} {
		if _, err := io.WriteString(client, want); err != nil {
			t.Fatalf("sending echo payload: %v", err)
		}
		got := make([]byte, len(want))
		if _, err := io.ReadFull(client, got); err != nil {
			t.Fatalf("reading echo for %q: %v", want, err)
		}
		if string(got) != want {
			t.Fatalf("echo: want %q, got %q", want, got)
		}
	}
}
