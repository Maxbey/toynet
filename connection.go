package toynet

import "fmt"

type connection struct {
	inputScratch []byte
	viewScratch  []byte

	pool      *memoryPool
	inputBuf  buffer
	outputBuf buffer
}

func newConnection(viewScratch []byte, pool *memoryPool) *connection {
	return &connection{
		viewScratch: viewScratch,

		pool:      pool,
		inputBuf:  newRingBuffer(pool),
		outputBuf: newRingBuffer(pool),
	}
}

func (c *connection) Peek(n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("peeking negative amount of bytes from the buffer: %d", n)
	}

	// check if there is something in the input buf ring, return as early as possible
	left, right := c.inputBuf.Readable()
	if n <= len(left) {
		return left[:n], nil
	}

	if c.inputBuf.size == 0 && n <= len(c.inputScratch) {
		return c.inputScratch[:n], nil
	}

	bufferred := c.inputBuf.size + len(c.inputScratch)
	if bufferred == 0 || n == 0 {
		return nil, nil
	}
	consumable := min(n, bufferred)
	// Even if n > default view buf, check if readable actually fits the default view buf
	if consumable > cap(c.viewScratch) {
		b, err := c.pool.Acquire(consumable)
		if err != nil {
			return nil, err
		}
		c.viewScratch = *b
	}

	sources := [][]byte{left, right, c.inputScratch}
	shift := 0
	for _, src := range sources {
		take := min(len(src), consumable)
		if consumable == 0 {
			break
		}

		copy(c.viewScratch[shift:], src[:take])
		shift += take
		consumable -= take
	}

	return c.viewScratch[:shift], nil
}

func (c *connection) PeekAll() ([]byte, error) {
	bufferred := c.inputBuf.size + len(c.inputScratch)

	return c.Peek(bufferred)
}

func (c *connection) Ack(n int) error {
	if n <= 0 {
		return fmt.Errorf("acknowledging invalid amount of bytes from the buffer: %d", n)
	}

	ackable := min(c.inputBuf.size+len(c.inputScratch), n)
	if n > ackable {
		return fmt.Errorf("acknowledging more bytes than buffer contains, ring: %d, scratch: %d, n: %d", c.inputBuf.size, len(c.inputScratch), n)
	}

	ringAckable := min(c.inputBuf.size, n)
	if ringAckable > 0 {
		if err := c.inputBuf.AdvanceRead(ringAckable); err != nil {
			return err
		}
		ackable -= ringAckable
	}

	c.inputScratch = c.inputScratch[ackable:]

	return nil
}

func (c *connection) Write(b []byte) error {
	return c.outputBuf.Write(b)
}

func (c *connection) ringifyInputScratch() error {
	return c.inputBuf.Write(c.inputScratch)
}

func (c *connection) setInputScratch(b []byte) {
	c.inputScratch = b
}

func (c *connection) close() {
	c.inputBuf.Free()
	c.outputBuf.Free()
}
