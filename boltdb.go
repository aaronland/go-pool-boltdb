package boltdb

import (
	"context"
	"errors"
	"github.com/aaronland/go-pool"
	"github.com/boltdb/bolt"
	"net/url"
	"strconv"
)

func init() {
	ctx := context.Background()
	pl := NewBoltDBPool()
	pool.Register(ctx, "boltdb", pl)
}

type DeflateFunc func(pool.Item) (interface{}, error)

type InflateFunc func(interface{}) (pool.Item, error)

type BoltDBPool struct {
	pool.Pool
	db      *bolt.DB
	bucket  string
	inflate InflateFunc
	deflate DeflateFunc
}

func NewBoltDBPool() pool.Pool {
	pl := &BoltDBPool{}
	return pl
}

func (pl *BoltDBPool) Open(ctx context.Context, uri string) error {

	u, err := url.Parse(uri)

	if err != nil {
		return err
	}

	bucket := u.Host

	if bucket == "" {
		return errors.New("Missing bucket")
	}
	
	q := u.Query()
	dsn := q.Get("dsn")

	if dsn == "" {
		return errors.New("Missing dsn")
	}

	deflate := func(i pool.Item) (interface{}, error) {
		return i.String(), nil
	}

	inflate := func(rsp interface{}) (pool.Item, error) {

		b_int := rsp.([]byte)

		int, err := strconv.ParseInt(string(b_int), 10, 64)

		if err != nil {
			return nil, err
		}

		return pool.NewIntItem(int), nil
	}

	db, err := bolt.Open(dsn, 0600, nil)

	if err != nil {
		return err
	}

	tx, err := db.Begin(true)

	if err != nil {
		return err
	}

	defer tx.Rollback()

	_, err = tx.CreateBucketIfNotExists([]byte(bucket))

	if err != nil {
		return err
	}

	err = tx.Commit()

	if err != nil {
		return err
	}

	pl.db = db
	pl.bucket = bucket
	pl.inflate = inflate
	pl.deflate = deflate

	return nil
}

// basically the interface for pool.LIFOPool should be changed
// to expect errors all over the place but today that is not
// the case... (20181222/thisisaaronland)

func (pl *BoltDBPool) Length() int64 {

	count := int64(0)

	pl.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(pl.bucket))

		c := b.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count += 1
		}

		return nil
	})

	return count
}

func (pl *BoltDBPool) Push(pi pool.Item) {

	pl.db.Update(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(pl.bucket))

		i, err := pl.deflate(pi)

		if err != nil {
			return err
		}

		id, err := b.NextSequence()

		if err != nil {
			return err
		}

		k := strconv.FormatInt(int64(id), 10)
		v := i.(string)

		return b.Put([]byte(k), []byte(v))
	})
}

func (pl *BoltDBPool) Pop() (pool.Item, bool) {

	var pi pool.Item

	err := pl.db.Update(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(pl.bucket))
		c := b.Cursor()

		k, v := c.First()

		p, err := pl.inflate(v)

		if err != nil {
			return err
		}

		err = b.Delete(k)

		if err != nil {
			return err
		}

		pi = p
		return nil
	})

	if err != nil {
		return nil, false
	}

	return pi, true
}
