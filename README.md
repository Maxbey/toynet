# toynet

`toynet` is a toy, Linux-only TCP networking library written in Go. It uses epoll, nonblocking sockets, pooled ring buffers, and connection-scoped `Peek`/`Ack` input handling.

## Example

```go
package main

import (
	"context"
	"log"

	"github.com/Maxbey/toynet"
)

type echoHandler struct{}

func (echoHandler) OnReadable(conn toynet.Connection) error {
	input, err := conn.PeekAll()
	if err != nil {
		return err
	}
	if len(input) == 0 {
		return nil
	}
	if err := conn.Write(input); err != nil {
		return err
	}
	return conn.Ack(len(input))
}

func main() {
	err := toynet.Run(context.Background(), toynet.Config{
		Host:           "127.0.0.1",
		Port:           9000,
		Protocol:       toynet.TCP,
		MaxConnections: 1024,
		MaxBuffer:      4 << 20,
	}, echoHandler{})
	if err != nil {
		log.Fatal(err)
	}
}
```

## Development

Run the tests on Linux:

```sh
go test ./...
```

Run the echo benchmarks:

```sh
cd benchmarks
go test -run '^$' -bench '^BenchmarkEchoRoundTrip' -benchmem
```
