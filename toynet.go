package toynet

import "context"

// Protocol identifies a transport protocol supported by Run.
type Protocol string

const (
	// TCP selects an IPv4 TCP listener.
	TCP Protocol = "TCP"
)

// Connection provides access to a client's buffered input and output.
//
// A Connection and any byte slice returned by Peek or PeekAll are valid only
// during the OnReadable call that supplied the Connection. Its methods must not
// be called concurrently.
type Connection interface {
	// Peek returns a contiguous view of up to n unconsumed input bytes. It
	// returns all available input when fewer than n bytes are buffered. The
	// returned view aliases connection-owned memory and must not be retained.
	// Peek returns an error when n is negative.
	Peek(int) ([]byte, error)

	// PeekAll returns a contiguous view of all currently buffered input. The
	// returned view aliases connection-owned memory and must not be retained.
	PeekAll() ([]byte, error)

	// Ack consumes n bytes from the beginning of the buffered input. It returns
	// an error unless n is positive and no greater than the available input.
	Ack(int) error

	// Write accepts b for transmission to the client. When Write returns nil,
	// the caller may immediately reuse b. If Write returns an error, a prefix of
	// b may already have been written to the socket.
	Write([]byte) error
}

// Handler receives readable connection events. Calls are serialized by the
// server's event loop. Returning an error closes the affected connection.
type Handler interface {
	// OnReadable handles the input currently available on c. The handler may
	// call Peek, Ack, and Write multiple times before returning.
	OnReadable(Connection) error
}

// Config configures a server started by Run.
type Config struct {
	// Host is the numeric IPv4 address on which to listen.
	Host string
	// Port is the TCP port on which to listen. Port zero asks the operating
	// system to select an available port.
	Port int

	// Protocol selects the transport protocol. TCP is currently the only
	// supported value.
	Protocol Protocol
	// MaxConnections is passed to the operating system as the listener backlog;
	// it is not a hard limit on established connections.
	MaxConnections int
	// MaxBuffer is the maximum capacity, in bytes, of an individual pooled
	// connection buffer.
	MaxBuffer int
}

// Run serves connections until ctx is canceled or the server encounters a
// fatal error. handler must be non-nil. Run is supported only on Linux.
func Run(ctx context.Context, config Config, handler Handler) error {
	s, err := newServer(config, handler)
	if err != nil {
		return err
	}

	return s.Run(ctx)
}
