//go:build !linux

package toynet

import (
	"context"
	"errors"
)

type server struct{}

func newServer(_ Config, _ Handler) (*server, error) {
	return &server{}, nil
}

func (server) Run(_ context.Context) error {
	return errors.New("toynet: unsupported platform")
}
