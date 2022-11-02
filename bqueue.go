package bqueue

import (
	"encoding/binary"
	"sync"

	"go.etcd.io/bbolt"
)

type Queue struct {
	db    *bbolt.DB
	mutex sync.Mutex
}

var (
	defaultQueueBucket = []byte("queue:default")
)

func itob(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}

func New(path string) (q *Queue, err error) {
	q = new(Queue)

	q.db, err = bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	q.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(defaultQueueBucket)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return
}

func (q *Queue) Close() error {
	return q.db.Close()
}

func (q *Queue) Push(payload []byte) (err error) {
	q.mutex.Lock()
	err = q.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultQueueBucket)

		i, err := b.NextSequence()
		if err != nil {
			return err
		}

		if err = b.Put(itob(i), payload); err != nil {
			return err
		}

		return nil
	})
	q.mutex.Unlock()

	return
}

func (q *Queue) Pop() (payload []byte, err error) {
	q.mutex.Lock()
	err = q.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultQueueBucket)

		k, v := b.Cursor().First()

		if err = b.Delete(k); err != nil {
			return err
		}

		payload = v

		return nil
	})
	q.mutex.Unlock()

	return
}
