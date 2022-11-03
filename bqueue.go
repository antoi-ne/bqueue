package bqueue

import (
	"encoding/binary"
	"os"
	"sync"

	"go.etcd.io/bbolt"
)

type Store struct {
	db    *bbolt.DB
	mutex sync.Mutex
}

type Queue struct {
	store *Store
	name  []byte
}

func Open(path string, mode os.FileMode, opts *bbolt.Options) (s *Store, err error) {
	s = new(Store)

	s.db, err = bbolt.Open(path, mode, opts)
	if err != nil {
		return nil, err
	}

	return
}

func (s *Store) Close() (err error) {
	s.mutex.Lock()
	err = s.db.Close()
	s.mutex.Unlock()

	return
}

func (s *Store) NewQueue(name []byte) (q *Queue, err error) {
	q = new(Queue)
	q.name = name
	q.store = s

	q.store.mutex.Lock()
	defer q.store.mutex.Unlock()

	if err = q.store.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(q.name)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return
}

func (q *Queue) Enqueue(message []byte) (err error) {
	q.store.mutex.Lock()
	defer q.store.mutex.Unlock()

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

	return
}

func (q *Queue) Dequeue() (message []byte, err error) {
	q.store.mutex.Lock()
	defer q.store.mutex.Unlock()

	err = q.store.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(q.name)

		k, v := b.Cursor().First()

		if err = b.Delete(k); err != nil {
			return err
		}

		message = v

		return nil
	})

	return
}

func itob(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}
