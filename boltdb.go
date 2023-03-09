package boltdb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/aaronland/go-pool/v2"
	"github.com/boltdb/bolt"
)

const BOLTDB_SCHEME string = "boltdb"

func init() {
	ctx := context.Background()
	pool.RegisterPool(ctx, BOLTDB_SCHEME, NewBoltDBPool)
}

type BoltDBPool struct {
	pool.Pool
	db     *bolt.DB
	bucket string
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
		db:     db,
		bucket: bucket,
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

		id, err := b.NextSequence()

		if err != nil {
			return err
		}

		k := strconv.FormatInt(int64(id), 10)

		v, err := json.Marshal(i)

		if err != nil {
			return fmt.Errorf("Failed to marshal data, %w", err)
		}

		return b.Put([]byte(k), v)
	})

	return err
}

func (pl *BoltDBPool) Pop(ctx context.Context) (any, bool) {

	var i any

	err := pl.db.Update(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(pl.bucket))
		c := b.Cursor()

		k, v := c.Last()

		err := json.Unmarshal(v, &i)

		if err != nil {
			return fmt.Errorf("Failed to unmarshal datam %w", err)
		}

		err = b.Delete(k)

		if err != nil {
			return fmt.Errorf("Failed to delete key, %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, false
	}

	return i, true
}
