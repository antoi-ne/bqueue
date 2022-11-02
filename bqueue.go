package bqueue

import (
	"encoding/binary"
	"os"
	"sync"

	"go.etcd.io/bbolt"
)

type Store struct {
	db *bbolt.DB
	mu sync.Mutex
}

type Queue struct {
	store *Store
	name  []byte
}

func NewStore(path string, mode os.FileMode, opts *bbolt.Options) (s *Store, err error) {
	s = new(Store)

	s.db, err = bbolt.Open(path, mode, opts)
	if err != nil {
		return nil, err
	}

	return
}

func (s *Store) Close() (err error) {
	s.mu.Lock()
	err = s.db.Close()
	s.mu.Unlock()

	return
}

func (s *Store) NewQueue(name []byte) (q *Queue, err error) {
	q = new(Queue)
	q.name = name
	q.store = s

	q.store.mu.Lock()
	err = q.store.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(q.name)
		if err != nil {
			return err
		}

		return nil
	})
	q.store.mu.Unlock()
	if err != nil {
		return nil, err
	}

	return
}

func (q *Queue) Push(message []byte) (err error) {
	q.store.mu.Lock()
	err = q.store.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(q.name)

		i, err := b.NextSequence()
		if err != nil {
			return err
		}

		if err = b.Put(itob(i), message); err != nil {
			return err
		}

		return nil
	})
	q.store.mu.Unlock()

	return
}

func (q *Queue) Pop() (message []byte, err error) {
	q.store.mu.Lock()
	err = q.store.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(q.name)

		k, v := b.Cursor().First()

		if err = b.Delete(k); err != nil {
			return err
		}

		message = v

		return nil
	})
	q.store.mu.Unlock()

	return
}

func itob(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}
