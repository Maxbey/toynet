package toynet

import "testing"

var (
	benchmarkBytes []byte
	benchmarkByte  byte
)

func BenchmarkBufferReuseVsMake(b *testing.B) {
	const payloadSize = 1 << 10
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	b.Run("ring_buffer", func(b *testing.B) {
		pool, err := newMemoryPool(2 << 20)
		if err != nil {
			b.Fatal(err)
		}
		ring := buffer{pool: pool}
		if err := ring.alloc(payloadSize); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.SetBytes(payloadSize)
		b.ResetTimer()
		for b.Loop() {
			first, second := ring.writable()
			written := copy(first, payload)
			written += copy(second, payload[written:])
			if err := ring.advanceWrite(written); err != nil {
				b.Fatal(err)
			}

			first, second = ring.Readable()
			benchmarkByte = first[0] ^ first[len(first)-1]
			if len(second) > 0 {
				benchmarkByte ^= second[0] ^ second[len(second)-1]
			}
			if err := ring.AdvanceRead(written); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if err := pool.Release(&ring.data); err != nil {
			b.Fatal(err)
		}
	})

	b.Run("make", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(payloadSize)
		for b.Loop() {
			buf := make([]byte, payloadSize)
			copy(buf, payload)
			benchmarkByte = buf[0] ^ buf[len(buf)-1]
			benchmarkBytes = buf
		}
	})
}
