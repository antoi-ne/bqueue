# bqueue
Persistent embedded FIFO queue implementation built on boltDB

## installation

```sh
$ go get pkg.coulon.dev/bqueue
```

## Usage

```go
package main

import (
	"fmt"
	"log"

	"pkg.coulon.dev/bqueue"
)

func main() {
	s, err := bqueue.NewStore("queue.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	q, err := s.NewQueue([]byte("default"))
	if err != nil {
		log.Fatal(err)
	}

	err = q.Push([]byte("Hello World!"))
	if err != nil {
		log.Fatal(err)
	}

	m, err := q.Pop()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("message: %s\n", m)
}
```

## Roadmap

* [x] Persistent FIFO queue
* [x] Multiple queues on the same db
* [x] Thread-safety
* [ ] Adding metadata to payloads