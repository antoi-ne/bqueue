package bqueue_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"go.etcd.io/bbolt"
	"pkg.coulon.dev/bqueue"
)

func TestOpen(t *testing.T) {
	d := t.TempDir()
	f := filepath.Join(d, "queue.db")
	s, err := bqueue.Open(f, 0600, nil)
	if err != nil {
		t.Fatal(err)
	} else if s == nil {
		t.Fatal("unexpected nil")
	}
	s.Close()
}

func TestOpenWrongPath(t *testing.T) {
	d := t.TempDir()
	f := filepath.Join(d, "fake-directory", "queue.db")
	_, err := bqueue.Open(f, 0600, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewQueue(t *testing.T) {
	d := t.TempDir()
	f := filepath.Join(d, "queue.db")
	s, err := bqueue.Open(f, 0600, nil)
	if err != nil {
		t.Fatal(err)
	} else if s == nil {
		t.Fatal("unexpected nil for store")
	}
	defer s.Close()

	q, err := s.NewQueue([]byte("foobar"))
	if err != nil {
		t.Fatal(err)
	} else if q == nil {
		t.Fatal("unexpected nil for queue")
	}
}

func TestNewQueueEmptyName(t *testing.T) {
	d := t.TempDir()
	f := filepath.Join(d, "queue.db")
	s, err := bqueue.Open(f, 0600, nil)
	if err != nil {
		t.Fatal(err)
	} else if s == nil {
		t.Fatal("unexpected nil for store")
	}
	defer s.Close()

	_, err = s.NewQueue(nil)
	if err != bbolt.ErrBucketNameRequired {
		t.Fatal("expected error")
	}
}

func TestNewQueueTwiceSameName(t *testing.T) {
	d := t.TempDir()
	f := filepath.Join(d, "queue.db")
	s, err := bqueue.Open(f, 0600, nil)
	if err != nil {
		t.Fatal(err)
	} else if s == nil {
		t.Fatal("unexpected nil for store")
	}
	defer s.Close()

	q1, err := s.NewQueue([]byte("foobar"))
	if err != nil {
		t.Fatal(err)
	} else if q1 == nil {
		t.Fatal("unexpected nil for queue")
	}

	q2, err := s.NewQueue([]byte("foobar"))
	if err != nil {
		t.Fatal(err)
	} else if q2 == nil {
		t.Fatal("unexpected nil for queue")
	}
}

func TestEnqueueDequeue(t *testing.T) {
	d := t.TempDir()
	f := filepath.Join(d, "queue.db")
	s, err := bqueue.Open(f, 0600, nil)
	if err != nil {
		t.Fatal(err)
	} else if s == nil {
		t.Fatal("unexpected nil for store")
	}
	defer s.Close()

	q, err := s.NewQueue([]byte("foobar"))
	if err != nil {
		t.Fatal(err)
	} else if q == nil {
		t.Fatal("unexpected nil for queue")
	}

	if err = q.Enqueue([]byte("message1")); err != nil {
		t.Fatal(err)
	}

	m, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	} else if bytes.Compare(m, []byte("message1")) != 0 {
		t.Fatal("dequeue output does not match inital message")
	}
}

func TestMultipleEnqueues(t *testing.T) {
	d := t.TempDir()
	f := filepath.Join(d, "queue.db")
	s, err := bqueue.Open(f, 0600, nil)
	if err != nil {
		t.Fatal(err)
	} else if s == nil {
		t.Fatal("unexpected nil for store")
	}
	defer s.Close()

	q, err := s.NewQueue([]byte("foobar"))
	if err != nil {
		t.Fatal(err)
	} else if q == nil {
		t.Fatal("unexpected nil for queue")
	}

	if err = q.Enqueue([]byte("message1")); err != nil {
		t.Fatal(err)
	}
	if err = q.Enqueue([]byte("message2")); err != nil {
		t.Fatal(err)
	}
	if err = q.Enqueue([]byte("message3")); err != nil {
		t.Fatal(err)
	}

	m, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	} else if bytes.Compare(m, []byte("message1")) != 0 {
		t.Fatal("dequeue output does not match inital message")
	}
	m, err = q.Dequeue()
	if err != nil {
		t.Fatal(err)
	} else if bytes.Compare(m, []byte("message2")) != 0 {
		t.Fatal("dequeue output does not match inital message")
	}
	m, err = q.Dequeue()
	if err != nil {
		t.Fatal(err)
	} else if bytes.Compare(m, []byte("message3")) != 0 {
		t.Fatal("dequeue output does not match inital message")
	}
}

func TestDequeueEmpty(t *testing.T) {
	d := t.TempDir()
	f := filepath.Join(d, "queue.db")
	s, err := bqueue.Open(f, 0600, nil)
	if err != nil {
		t.Fatal(err)
	} else if s == nil {
		t.Fatal("unexpected nil for store")
	}
	defer s.Close()

	q, err := s.NewQueue([]byte("foobar"))
	if err != nil {
		t.Fatal(err)
	} else if q == nil {
		t.Fatal("unexpected nil for queue")
	}

	m, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	} else if m != nil {
		t.Fatal("dequeue output of empty queue should be nil")
	}
}

func TestQueuePersistence(t *testing.T) {
	d := t.TempDir()
	f := filepath.Join(d, "queue.db")
	s, err := bqueue.Open(f, 0600, nil)
	if err != nil {
		t.Fatal(err)
	} else if s == nil {
		t.Fatal("unexpected nil for store")
	}
	defer s.Close()

	q, err := s.NewQueue([]byte("foobar"))
	if err != nil {
		t.Fatal(err)
	} else if q == nil {
		t.Fatal("unexpected nil for queue")
	}

	if err = q.Enqueue([]byte("message1")); err != nil {
		t.Fatal(err)
	}

	s.Close()

	s, err = bqueue.Open(f, 0600, nil)
	if err != nil {
		t.Fatal(err)
	} else if s == nil {
		t.Fatal("unexpected nil for store")
	}
	defer s.Close()

	q, err = s.NewQueue([]byte("foobar"))
	if err != nil {
		t.Fatal(err)
	} else if q == nil {
		t.Fatal("unexpected nil for queue")
	}

	m, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	} else if bytes.Compare(m, []byte("message1")) != 0 {
		t.Fatal("dequeue output does not match inital message")
	}
}
