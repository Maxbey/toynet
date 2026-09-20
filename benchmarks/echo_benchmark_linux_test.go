//go:build linux

package benchmarks

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Maxbey/toynet"
	"github.com/panjf2000/gnet/v2"
)

const echoBenchmarkTimeout = 5 * time.Minute
const echoBenchmarkConnections = 100

type benchmarkDiscardLogger struct{}

func (benchmarkDiscardLogger) Debugf(string, ...any) {}
func (benchmarkDiscardLogger) Infof(string, ...any)  {}
func (benchmarkDiscardLogger) Warnf(string, ...any)  {}
func (benchmarkDiscardLogger) Errorf(string, ...any) {}
func (benchmarkDiscardLogger) Fatalf(string, ...any) {}

type toynetEchoBenchmarkHandler struct{}

func (toynetEchoBenchmarkHandler) OnReadable(c toynet.Connection) error {
	input, err := c.PeekAll()
	if err != nil || len(input) == 0 {
		return err
	}
	if err := c.Write(input); err != nil {
		return err
	}
	return c.Ack(len(input))
}

type gnetEchoBenchmarkHandler struct {
	gnet.BuiltinEventEngine
	ready chan gnet.Engine
}

func (h *gnetEchoBenchmarkHandler) OnBoot(engine gnet.Engine) gnet.Action {
	h.ready <- engine
	return gnet.None
}

func (*gnetEchoBenchmarkHandler) OnTraffic(c gnet.Conn) gnet.Action {
	input, err := c.Next(-1)
	if err != nil {
		return gnet.Close
	}
	if _, err := c.Write(input); err != nil {
		return gnet.Close
	}
	return gnet.None
}

func BenchmarkEchoRoundTrip(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{name: "256B", size: 256},
		{name: "512B", size: 512},
		{name: "1MiB", size: 1 << 20},
		{name: "2MiB", size: 2 << 20},
	}

	for _, size := range sizes {
		b.Run(size.name+"/toynet", func(b *testing.B) {
			benchmarkEchoServer(b, size.size, startToynetEchoBenchmark)
		})
		b.Run(size.name+"/gnet", func(b *testing.B) {
			benchmarkEchoServer(b, size.size, startGnetEchoBenchmark)
		})
	}
}

func BenchmarkEchoRoundTrip100Connections(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{name: "256B", size: 256},
		{name: "512B", size: 512},
		{name: "1MiB", size: 1 << 20},
		{name: "2MiB", size: 2 << 20},
	}

	for _, size := range sizes {
		b.Run(size.name+"/toynet", func(b *testing.B) {
			benchmarkConcurrentEchoServer(b, size.size, echoBenchmarkConnections, startToynetEchoBenchmark)
		})
		b.Run(size.name+"/gnet", func(b *testing.B) {
			benchmarkConcurrentEchoServer(b, size.size, echoBenchmarkConnections, startGnetEchoBenchmark)
		})
	}
}

type echoBenchmarkStarter func(testing.TB) (net.Conn, func())

func benchmarkEchoServer(b *testing.B, size int, start echoBenchmarkStarter) {
	client, stop := start(b)
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	received := make([]byte, size)
	if err := client.SetDeadline(time.Now().Add(echoBenchmarkTimeout)); err != nil {
		stop()
		b.Fatal(err)
	}
	if err := echoBenchmarkRoundTrip(client, payload, received); err != nil {
		stop()
		b.Fatalf("warm-up round trip: %v", err)
	}
	if !bytes.Equal(received, payload) {
		stop()
		b.Fatal("warm-up echo differs from payload")
	}

	b.ReportAllocs()
	b.SetBytes(int64(size) * 2)
	for b.Loop() {
		if err := echoBenchmarkRoundTrip(client, payload, received); err != nil {
			b.Fatal(err)
		}
	}
	stop()
}

func benchmarkConcurrentEchoServer(b *testing.B, size, connectionCount int, start echoBenchmarkStarter) {
	b.StopTimer()
	first, stop := start(b)
	clients := make([]net.Conn, 0, connectionCount)
	clients = append(clients, first)
	address := first.RemoteAddr().String()
	for len(clients) < connectionCount {
		client, err := net.DialTimeout("tcp4", address, 2*time.Second)
		if err != nil {
			for _, existing := range clients[1:] {
				_ = existing.Close()
			}
			stop()
			b.Fatalf("opening benchmark connection %d: %v", len(clients)+1, err)
		}
		clients = append(clients, client)
	}
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	received := make([][]byte, connectionCount)
	for i, client := range clients {
		received[i] = make([]byte, size)
		if err := client.SetDeadline(time.Now().Add(echoBenchmarkTimeout)); err != nil {
			b.Fatal(err)
		}
		if err := echoBenchmarkRoundTrip(client, payload, received[i]); err != nil {
			b.Fatalf("warming connection %d: %v", i, err)
		}
		if !bytes.Equal(received[i], payload) {
			b.Fatalf("warm-up echo differs on connection %d", i)
		}
	}

	var next atomic.Uint64
	startWork := make(chan struct{})
	errorsFound := make(chan error, 1)
	var workers sync.WaitGroup
	workers.Add(connectionCount)
	for i, client := range clients {
		response := received[i]
		go func() {
			defer workers.Done()
			<-startWork
			for {
				operation := next.Add(1)
				if operation > uint64(b.N) {
					return
				}
				if err := echoBenchmarkRoundTrip(client, payload, response); err != nil {
					select {
					case errorsFound <- err:
					default:
					}
					return
				}
			}
		}()
	}

	b.ReportAllocs()
	b.SetBytes(int64(size) * 2)
	b.ResetTimer()
	b.StartTimer()
	close(startWork)
	workers.Wait()
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "req/s")

	for _, client := range clients[1:] {
		_ = client.Close()
	}
	stop()
	select {
	case err := <-errorsFound:
		b.Fatal(err)
	default:
	}
}

func echoBenchmarkRoundTrip(client net.Conn, payload, received []byte) error {
	if _, err := client.Write(payload); err != nil {
		return fmt.Errorf("writing request: %w", err)
	}
	if _, err := io.ReadFull(client, received); err != nil {
		return fmt.Errorf("reading response: %w", err)
	}
	return nil
}

func reserveEchoBenchmarkAddress(tb testing.TB) (string, int) {
	tb.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		tb.Fatal(err)
	}
	return net.JoinHostPort("127.0.0.1", fmt.Sprint(port)), port
}

func dialEchoBenchmark(tb testing.TB, address string, done <-chan error) net.Conn {
	tb.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-done:
			tb.Fatalf("server exited before accepting a benchmark client: %v", err)
		default:
		}
		client, err := net.DialTimeout("tcp4", address, 100*time.Millisecond)
		if err == nil {
			return client
		}
		if time.Now().After(deadline) {
			tb.Fatalf("connecting to benchmark server: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func startToynetEchoBenchmark(tb testing.TB) (net.Conn, func()) {
	tb.Helper()
	address, port := reserveEchoBenchmarkAddress(tb)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- toynet.Run(ctx, toynet.Config{
			Host:           "127.0.0.1",
			Port:           port,
			Protocol:       toynet.TCP,
			MaxConnections: 256,
			MaxBuffer:      4 << 20,
		}, toynetEchoBenchmarkHandler{})
	}()
	client := dialEchoBenchmark(tb, address, done)

	return client, func() {
		_ = client.Close()
		cancel()
		select {
		case err := <-done:
			if err != nil {
				tb.Errorf("stopping toynet benchmark server: %v", err)
			}
		case <-time.After(2 * time.Second):
			tb.Error("toynet benchmark server did not stop")
		}
	}
}

func startGnetEchoBenchmark(tb testing.TB) (net.Conn, func()) {
	tb.Helper()
	address, _ := reserveEchoBenchmarkAddress(tb)
	handler := &gnetEchoBenchmarkHandler{ready: make(chan gnet.Engine, 1)}
	done := make(chan error, 1)
	go func() {
		done <- gnet.Run(
			handler,
			"tcp4://"+address,
			gnet.WithMulticore(false),
			gnet.WithTCPNoDelay(gnet.TCPDelay),
			gnet.WithLogger(benchmarkDiscardLogger{}),
		)
	}()

	var engine gnet.Engine
	select {
	case engine = <-handler.ready:
	case err := <-done:
		tb.Fatalf("gnet benchmark server failed to start: %v", err)
	case <-time.After(2 * time.Second):
		tb.Fatal("gnet benchmark server did not start")
	}
	client := dialEchoBenchmark(tb, address, done)

	return client, func() {
		_ = client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := engine.Stop(ctx); err != nil {
			tb.Errorf("stopping gnet benchmark server: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				tb.Errorf("gnet benchmark server: %v", err)
			}
		case <-time.After(2 * time.Second):
			tb.Error("gnet benchmark server did not stop")
		}
	}
}
