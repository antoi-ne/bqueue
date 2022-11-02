package bqueue_test

import (
	"path/filepath"
	"testing"

	"pkg.coulon.dev/bqueue"
)

func TestNewStore(t *testing.T) {
	d := t.TempDir()
	f := filepath.Join(d, "queue.db")
	s, err := bqueue.NewStore(f, 0600, nil)
	if err != nil {
		t.Fatal(err)
	} else if s == nil {
		t.Fatal("unexpected nil")
	}
	s.Close()
}

func TestNewQueue(t *testing.T) {
	d := t.TempDir()
	f := filepath.Join(d, "queue.db")
	s, err := bqueue.NewStore(f, 0600, nil)
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
