# bqueue
Persistent FIFO queue implementation built on boltDB

## installation

```sh
$ go get pkg.coulon.dev/bqueue
```

## Usage

```go
package main

import (
	"log"

	"pkg.coulon.dev/bqueue"
)

func main() {
	q, _ := bqueue.New("queue.db")
	defer q.Close()

	q.Push([]byte("Hello World"))

	qdata, _ := q.Pop()
}
```

## Roadmap

* [x] Persistent FIFO queue
* [] Multiple queues on the same db
* [] Thread-safety
* [] Adding metadata to payloads
