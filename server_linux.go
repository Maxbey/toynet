//go:build linux

package toynet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/netip"

	"golang.org/x/sys/unix"
)

const (
	epollEvents   = 1024
	epollTimeout  = 100
	syscallBudget = 10
)

type connectionRegistration struct {
	conn   *linuxConnection
	events uint32
}

type server struct {
	cfg     Config
	handler Handler
	pool    *memoryPool

	connections map[int]connectionRegistration
}

func newServer(cfg Config, handler Handler) (*server, error) {
	p, err := newMemoryPool(cfg.MaxBuffer)
	if err != nil {
		return nil, fmt.Errorf("creating server: %w", err)
	}

	return &server{
		connections: make(map[int]connectionRegistration, 1024),
		handler:     handler,
		cfg:         cfg,
		pool:        p,
	}, nil
}

func (s *server) Run(ctx context.Context) error {
	return s.run(ctx)
}

func (s *server) run(ctx context.Context) error {
	sfd, err := s.bindSocket(s.cfg.Protocol)
	if err != nil {
		return fmt.Errorf("binding a socket: %w", err)
	}
	defer unix.Close(sfd)

	if err := unix.Listen(sfd, s.cfg.MaxConnections); err != nil {
		return fmt.Errorf("listening on socket: %w", err)
	}
	poller, err := newEpoll()
	if err != nil {
		return err
	}
	defer poller.close()

	if err := poller.add(sfd, unix.EPOLLIN); err != nil {
		return fmt.Errorf("registering listener with epoll: %w", err)
	}

	return s.loop(ctx, poller, sfd)
}

func (s *server) loop(ctx context.Context, poll epoll, socketFd int) error {
	events := make([]unix.EpollEvent, epollEvents)
	inputScratch := make([]byte, 64<<10)
	viewScratch := make([]byte, 64<<10)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		n, err := poll.wait(events, epollTimeout)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return fmt.Errorf("waiting for epoll events: %w", err)
		}

		for _, e := range events[:n] {
			if e.Fd == int32(socketFd) {
				if err := s.acceptConnection(poll, socketFd, viewScratch, syscallBudget); err != nil {
					slog.Warn("failure accepting connection", "err", err)
					continue
				}
				continue
			}

			reg := s.connections[int(e.Fd)]
			if e.Events&unix.EPOLLIN != 0 {
				if err := s.readConn(reg.conn, inputScratch); err != nil {
					s.closeConn(reg.conn, poll)

					continue
				}
			}
			if e.Events&unix.EPOLLOUT != 0 {
				if err := s.writeConn(reg.conn); err != nil {
					s.closeConn(reg.conn, poll)

					continue
				}
			}

			updatedEvents := 0
			if !reg.conn.EOF {
				updatedEvents |= unix.EPOLLIN
			}
			if reg.conn.state.outputBuf.size > 0 {
				updatedEvents |= unix.EPOLLOUT
			}

			if updatedEvents == 0 {
				s.closeConn(reg.conn, poll)
				continue
			}

			if reg.events != uint32(updatedEvents) {
				if err := poll.update(int(e.Fd), uint32(updatedEvents)); err != nil {
					s.closeConn(reg.conn, poll)

					continue
				}

				reg.events = uint32(updatedEvents)
				s.connections[int(e.Fd)] = reg
			}
		}
	}
}

func (s *server) writeConn(conn *linuxConnection) error {
	if conn.state.outputBuf.size == 0 {
		return nil
	}

	left, right := conn.state.outputBuf.Readable()
	toWrite := len(left)

	n, err := socketWrite(conn.fd, left)
	if err != nil {
		return err
	}
	if err := conn.state.outputBuf.AdvanceRead(n); err != nil {
		return err
	}
	if n != toWrite || len(right) == 0 {
		return nil
	}

	n, err = socketWrite(conn.fd, right)
	if err != nil {
		return err
	}

	return conn.state.outputBuf.AdvanceRead(n)
}

func (s *server) readConn(conn *linuxConnection, inputScratch []byte) error {
	n, err := socketRead(conn.fd, inputScratch)
	if errors.Is(err, io.EOF) {
		conn.EOF = true
	} else if err != nil {
		return err
	}

	conn.state.setInputScratch(inputScratch[:n])
	if err := s.handler.OnReadable(conn); err != nil {
		return err
	}
	if err := conn.state.ringifyInputScratch(); err != nil {
		return err
	}
	conn.state.setInputScratch(inputScratch[:0])

	return nil
}

func (s *server) closeConn(conn *linuxConnection, poll epoll) {
	unix.Close(conn.fd)

	conn.state.close()
	delete(s.connections, conn.fd)

	poll.remove(conn.fd)
}

func (s *server) acceptConnection(poll epoll, sfd int, viewScratch []byte, budget int) error {
	calls := 0

	for calls < budget {
		fd, _, err := unix.Accept4(
			sfd,
			unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC,
		)
		calls += 1

		switch {
		case errors.Is(err, unix.EAGAIN):
			return nil
		case errors.Is(err, unix.EINTR):
			continue
		case err != nil:
			return fmt.Errorf("accepting connection: %w", err)
		default:
		}

		if err := poll.add(fd, unix.EPOLLIN); err != nil {
			unix.Close(fd)
			return err
		}

		s.connections[fd] = connectionRegistration{
			conn:   newLinuxConnection(fd, viewScratch, s.pool),
			events: unix.EPOLLIN,
		}
	}

	return nil
}

func (s *server) bindSocket(protocol Protocol) (fd int, err error) {
	if protocol != TCP {
		return -1, fmt.Errorf("toynet: unsupported protocol %q", protocol)
	}

	fd, err = unix.Socket(
		unix.AF_INET,
		unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC,
		0,
	)
	defer func() {
		if err != nil {
			unix.Close(fd)
		}
	}()
	if err != nil {
		return fd, fmt.Errorf("creating socket: %w", err)
	}

	err = unix.SetsockoptInt(
		fd,
		unix.SOL_SOCKET,
		unix.SO_REUSEADDR,
		1,
	)
	if err != nil {
		return fd, fmt.Errorf("setting SO_REUSEADDR: %w", err)
	}

	var address unix.SockaddrInet4
	address, err = addrInet4(s.cfg.Host, s.cfg.Port)
	if err != nil {
		return fd, err
	}
	if err = unix.Bind(fd, &address); err != nil {
		return fd, fmt.Errorf("binding socket: %w", err)
	}

	return fd, nil
}

func addrInet4(host string, port int) (unix.SockaddrInet4, error) {
	address, err := netip.ParseAddr(host)
	if err != nil {
		return unix.SockaddrInet4{}, fmt.Errorf("parsing IPv4 address %q: %w", host, err)
	}
	if !address.Is4() {
		return unix.SockaddrInet4{}, fmt.Errorf("address %q is not IPv4", host)
	}
	if port < 0 || port > 65535 {
		return unix.SockaddrInet4{}, fmt.Errorf("port must be between 0 and 65535: %d", port)
	}

	return unix.SockaddrInet4{
		Port: port,
		Addr: address.As4(),
	}, nil
}

type linuxConnection struct {
	fd    int
	state *connection
	EOF   bool
}

func newLinuxConnection(fd int, view []byte, pool *memoryPool) *linuxConnection {
	return &linuxConnection{
		fd:    fd,
		state: newConnection(view, pool),
	}

}

func (c *linuxConnection) Peek(n int) ([]byte, error) {
	return c.state.Peek(n)
}

func (c *linuxConnection) PeekAll() ([]byte, error) {
	return c.state.PeekAll()
}

func (c *linuxConnection) Ack(n int) error {
	return c.state.Ack(n)
}

func (c *linuxConnection) Write(b []byte) error {
	if c.state.outputBuf.size > 0 {
		return c.state.outputBuf.Write(b)
	}

	n, err := socketWrite(c.fd, b)
	if err != nil {
		return err
	}

	return c.state.outputBuf.Write(b[n:])
}

func socketRead(fd int, b []byte) (int, error) {
	var (
		n   int
		err error
	)
	calls := 0
	received := 0

	for received < len(b) {
		n, err = unix.Read(fd, b[received:])
		calls += 1
		if n > 0 {
			received += n
		}

		if isInterrupt(err) {
			break
		}
		if err != nil {
			return received, err
		}

		if n == 0 {
			return received, io.EOF
		}
	}

	return received, nil
}

func socketWrite(fd int, b []byte) (int, error) {
	var (
		n   int
		err error
	)
	calls := 0
	written := 0

	for len(b) > 0 {
		n, err = unix.Write(fd, b)
		calls += 1
		if n > 0 {
			written += n
			b = b[n:]
		}

		if isInterrupt(err) || n == 0 {
			break
		}

		if err != nil {
			return written, err
		}
	}

	return written, nil
}

func isInterrupt(err error) bool {
	return err == unix.EINTR || err == unix.EAGAIN || err == unix.EWOULDBLOCK
}
