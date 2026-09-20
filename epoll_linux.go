//go:build linux

package toynet

import (
	"fmt"

	"golang.org/x/sys/unix"
)

type epoll struct {
	fd int
}

func newEpoll() (epoll, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return epoll{}, fmt.Errorf("creating epoll instance: %w", err)
	}

	return epoll{fd: fd}, nil
}

func (e epoll) add(fd int, events uint32) error {
	event := unix.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	}

	return unix.EpollCtl(e.fd, unix.EPOLL_CTL_ADD, fd, &event)
}

func (e epoll) update(fd int, events uint32) error {
	event := unix.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	}

	return unix.EpollCtl(e.fd, unix.EPOLL_CTL_MOD, fd, &event)
}

func (e epoll) remove(fd int) error {
	return unix.EpollCtl(e.fd, unix.EPOLL_CTL_DEL, fd, nil)
}

func (e epoll) wait(events []unix.EpollEvent, timeout int) (int, error) {
	return unix.EpollWait(e.fd, events, timeout)
}

func (e epoll) close() error {
	if err := unix.Close(e.fd); err != nil {
		return fmt.Errorf("closing epoll instance: %w", err)
	}
	return nil
}
