package boltdb

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/aaronland/go-pool/v2"
	"github.com/boltdb/bolt"
)

func init() {
	ctx := context.Background()
	pool.RegisterPool(ctx, "boltdb", NewBoltDBPool)
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

func NewBoltDBPool(ctx context.Context, uri string) (pool.Pool, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	bucket := u.Host

	if bucket == "" {
		return nil, fmt.Errorf("Missing bucket")
	}

	q := u.Query()
	dsn := q.Get("dsn")

	if dsn == "" {
		return nil, fmt.Errorf("Missing dsn")
	}

	deflate := func(i pool.Item) (interface{}, error) {
		return i.String(), nil
	}

	inflate := func(rsp interface{}) (pool.Item, error) {

		b_int := rsp.([]byte)

		int, err := strconv.ParseInt(string(b_int), 10, 64)

		if err != nil {
			return nil, fmt.Errorf("Failed to parse pool item '%s', %w", string(b_int), err)
		}

		return pool.NewIntItem(int), nil
	}

	db, err := bolt.Open(dsn, 0600, nil)

	if err != nil {
		return nil, fmt.Errorf("Failed to open database, %w", err)
	}

	tx, err := db.Begin(true)

	if err != nil {
		return nil, fmt.Errorf("Failed to start transaction, %w", err)
	}

	defer tx.Rollback()

	_, err = tx.CreateBucketIfNotExists([]byte(bucket))

	if err != nil {
		return nil, fmt.Errorf("Failed to create bucket, %w", err)
	}

	err = tx.Commit()

	if err != nil {
		return nil, fmt.Errorf("Failed to commit transaction, %w", err)
	}

	pl := &BoltDBPool{
		db:      db,
		bucket:  bucket,
		inflate: inflate,
		deflate: deflate,
	}

	return pl, nil
}

func (pl *BoltDBPool) Length(ctx context.Context) int64 {

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

func (pl *BoltDBPool) Push(ctx context.Context, i any) error {

	err := pl.db.Update(func(tx *bolt.Tx) error {

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

	return err
}

func (pl *BoltDBPool) Pop(ctx context.Context) (any, bool) {

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
