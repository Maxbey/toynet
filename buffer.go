package toynet

import (
	"fmt"
)

type buffer struct {
	pool        *memoryPool
	data        []byte
	size        int
	readOffset  int
	writeOffset int
}

func NewBuffer(pool *memoryPool) buffer {
	return buffer{
		pool: pool,
	}
}

func (b *buffer) Readable() (first, second []byte) {
	if b.size == 0 {
		return nil, nil
	}

	if b.writeOffset <= b.readOffset {
		return b.data[b.readOffset:], b.data[:b.writeOffset]
	}

	return b.data[b.readOffset:b.writeOffset], nil
}

func (b *buffer) Write(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}

	if err := b.alloc(len(payload)); err != nil {
		return err
	}
	left, right := b.writable()

	toWrite := min(len(left), len(payload))
	copy(left, payload[:toWrite])
	copy(right, payload[toWrite:])

	if err := b.advanceWrite(len(payload)); err != nil {
		return err
	}

	return nil
}

func (b *buffer) Free() error {
	if b.data == nil {
		return nil
	}

	old := b.data
	b.data = nil
	b.size = 0
	b.readOffset = 0
	b.writeOffset = 0

	return b.pool.Release(&old)
}

func (b *buffer) AdvanceRead(n int) error {
	if n == 0 {
		return nil
	}

	if n < 0 {
		return fmt.Errorf("invalid delta advancing buffer read offset: %d", n)
	}

	l, r := b.Readable()
	limit := len(l) + len(r)

	if n > limit {
		return fmt.Errorf("advancing buffer read offset beyond limit: %d of %d", n, limit)
	}

	b.readOffset = (b.readOffset + n) % cap(b.data)
	b.size -= n

	return nil
}

func (b *buffer) advanceWrite(n int) error {
	if n == 0 {
		return nil
	}

	if n < 0 {
		return fmt.Errorf("invalid delta advancing buffer write offset: %d", n)
	}

	l, r := b.writable()
	limit := len(l) + len(r)

	if n > limit {
		return fmt.Errorf("advancing buffer write offset beyond limit: %d of %d", n, limit)
	}

	b.writeOffset = (b.writeOffset + n) % cap(b.data)
	b.size += n

	return nil
}

func (b *buffer) writable() (first, second []byte) {
	if b.available() == 0 {
		return nil, nil
	}

	if b.writeOffset >= b.readOffset {
		return b.data[b.writeOffset:], b.data[:b.readOffset]
	}

	return b.data[b.writeOffset:b.readOffset], nil
}

func (b *buffer) alloc(size int) error {
	if size <= 0 {
		return fmt.Errorf("acquire buffer: negative size allocation of %d", size)
	}

	available := b.available()
	if available >= size {
		return nil
	}

	newBuf, err := b.pool.Acquire(cap(b.data) + size - available)
	if err != nil {
		return fmt.Errorf("acquire buffer: %w", err)
	}
	new := *newBuf
	old := b.data

	if old != nil {
		if b.writeOffset <= b.readOffset && b.size > 0 {
			right := len(old) - b.readOffset
			copy(new[:right], old[b.readOffset:])
			copy(new[right:], old[:b.writeOffset])

			b.readOffset = 0
			b.writeOffset = right + b.writeOffset

		} else {
			copy(new, old)
		}
		if err := b.pool.Release(&old); err != nil {
			return fmt.Errorf("release buffer: %w", err)
		}
	}

	b.data = new
	return nil
}

func (b *buffer) Size() int {
	return b.size
}

func (b *buffer) available() int {
	if b.size == 0 {
		return cap(b.data)
	}

	if b.readOffset > b.writeOffset {
		return b.readOffset - b.writeOffset
	}
	if b.writeOffset > b.readOffset {
		return cap(b.data) - b.writeOffset + b.readOffset
	}

	return 0
}
