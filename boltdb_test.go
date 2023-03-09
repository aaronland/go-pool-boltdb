package boltdb

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/aaronland/go-pool/v2"
)

func TestMemoryPool(t *testing.T) {

	ctx := context.Background()

	f, err := os.CreateTemp("", "example")

	if err != nil {
		t.Fatalf("Failed to create temp file, %v", err)
	}

	defer os.Remove(f.Name())

	err = f.Close()

	if err != nil {
		t.Fatalf("Failed to close temp file, %v", err)
	}

	uri := fmt.Sprintf("%s://test?dsn=%s", BOLTDB_SCHEME, f.Name())

	pl, err := pool.NewPool(ctx, uri)

	if err != nil {
		t.Fatalf("Failed to create new pool for %s, %v", uri, err)
	}

	err = pl.Push(ctx, "a")

	if err != nil {
		t.Fatalf("Failed to add 'a', %v", err)
	}

	err = pl.Push(ctx, "b")

	if err != nil {
		t.Fatalf("Failed to add 'b', %v", err)
	}

	if pl.Length(ctx) != 2 {
		t.Fatalf("Expected length of 2, got %d", pl.Length(ctx))
	}

	v, ok := pl.Pop(ctx)

	if !ok {
		t.Fatalf("Failed to pop")
	}

	if v.(string) != "b" {
		t.Fatalf("Unexpected value %v (expected %s)", v, "b")
	}

	if pl.Length(ctx) != 1 {
		t.Fatalf("Expected length of 1, got %d", pl.Length(ctx))
	}

	v, ok = pl.Pop(ctx)

	if !ok {
		t.Fatalf("Failed to pop")
	}

	if v.(string) != "a" {
		t.Fatalf("Unexpected value %v (expected %s)", v, "a")
	}

	if pl.Length(ctx) != 0 {
		t.Fatalf("Expected length of 0, got %d", pl.Length(ctx))
	}

}
