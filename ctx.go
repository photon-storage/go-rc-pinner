package rcpinner

import (
	"context"

	"github.com/ipfs/boxo/ipld/merkledag"
	"go.uber.org/atomic"
)

const progressCtxKey = "progress"

func WithProgressTracker(
	ctx context.Context,
	pt *merkledag.ProgressTracker,
) context.Context {
	return context.WithValue(ctx, progressCtxKey, pt)
}

func ProgressTracker(ctx context.Context) *merkledag.ProgressTracker {
	v := ctx.Value(progressCtxKey)
	if v == nil {
		return nil
	}

	pt, ok := v.(*merkledag.ProgressTracker)
	if !ok {
		return nil
	}
	return pt
}

const dagSizeCtxKey = "dag_size"

func WithDagSize(
	ctx context.Context,
	ds *atomic.Uint64,
) context.Context {
	return context.WithValue(ctx, dagSizeCtxKey, ds)
}

func DagSize(ctx context.Context) *atomic.Uint64 {
	v := ctx.Value(dagSizeCtxKey)
	if v == nil {
		return nil
	}

	ds, ok := v.(*atomic.Uint64)
	if !ok {
		return nil
	}
	return ds
}

const concurrencyCtxKey = "concurrency"

func WithConcurrency(
	ctx context.Context,
	cc int,
) context.Context {
	return context.WithValue(ctx, concurrencyCtxKey, cc)
}

func Concurrency(ctx context.Context) int {
	v := ctx.Value(concurrencyCtxKey)
	if v == nil {
		return 0
	}

	cc, ok := v.(int)
	if !ok {
		return 0
	}
	return cc
}
