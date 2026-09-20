package toynet

import "context"

type Protocol string

const TCP Protocol = "TCP"

type Connection interface {
	Peek(int) ([]byte, error)
	PeekAll() ([]byte, error)
	Ack(int) error
	Write([]byte) error
}

type Handler interface {
	OnReadable(Connection) error
}

type Config struct {
	Host string
	Port int

	Protocol       Protocol
	MaxConnections int
	MaxBuffer      int
}

type Server interface {
	Run(context.Context, Config, Handler) error
}

func Run(ctx context.Context, config Config, handler Handler) error {
	s, err := newServer(config, handler)
	if err != nil {
		return err
	}

	return s.Run(ctx)
}
