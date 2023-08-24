package rcpinner

import (
	"context"
	"testing"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/photon-storage/go-common/testing/require"
	"go.uber.org/atomic"
)

func TestCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.Nil(t, ProgressTracker(ctx))
	ctx = WithProgressTracker(ctx, nil)
	require.Nil(t, ProgressTracker(ctx))
	ctx = WithProgressTracker(ctx, &merkledag.ProgressTracker{})
	require.NotNil(t, ProgressTracker(ctx))

	require.Nil(t, DagSize(ctx))
	ctx = WithDagSize(ctx, nil)
	require.Nil(t, DagSize(ctx))
	ctx = WithDagSize(ctx, atomic.NewUint64(0))
	require.NotNil(t, DagSize(ctx))

	require.Equal(t, 0, Concurrency(ctx))
	ctx = WithConcurrency(ctx, 0)
	require.Equal(t, 0, Concurrency(ctx))
	ctx = WithConcurrency(ctx, 11)
	require.Equal(t, 11, Concurrency(ctx))
}
