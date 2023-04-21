package rcpinner

import (
	"context"
	"testing"

	ds "github.com/ipfs/go-datastore"

	"github.com/photon-storage/go-common/testing/require"
)

func createIndex(ctx context.Context) *index {
	dstore := ds.NewMapDatastore()
	idx := newIndex(dstore, ds.NewKey("/data/test_index"))
	idx.add(ctx, "alice", "a1")
	idx.add(ctx, "bob", "b1")
	idx.add(ctx, "bob", "b2")
	idx.add(ctx, "cathy", "c1")
	return idx
}

func TestAdd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	idx := createIndex(ctx)
	require.NoError(t, idx.add(ctx, "someone", "s1"))
	require.NoError(t, idx.add(ctx, "someone", "s1"))
	require.ErrorIs(t, ErrEmptyKey, idx.add(ctx, "", "noindex"))
	require.ErrorIs(t, ErrEmptyValue, idx.add(ctx, "nokey", ""))
}

func TestDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	idx := createIndex(ctx)

	require.NoError(t, idx.del(ctx, "bob", "b3"))
	require.NoError(t, idx.del(ctx, "alice", "a1"))

	found, err := idx.HasValue(ctx, "alice", "a1")
	require.NoError(t, err)
	require.False(t, found)
	found, err = idx.HasValue(ctx, "bob", "b1")
	require.NoError(t, err)
	require.True(t, found)
	found, err = idx.HasValue(ctx, "bob", "b2")
	require.NoError(t, err)
	require.True(t, found)

	require.NoError(t, idx.delKey(ctx, "bob"))
	found, err = idx.HasValue(ctx, "bob", "b1")
	require.NoError(t, err)
	require.False(t, found)
	found, err = idx.HasValue(ctx, "bob", "b2")
	require.NoError(t, err)
	require.False(t, found)
}

func TestHasValue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	idx := createIndex(ctx)

	found, err := idx.HasValue(ctx, "bob", "b1")
	require.NoError(t, err)
	require.True(t, found)

	found, err = idx.HasValue(ctx, "bob", "b3")
	require.NoError(t, err)
	require.False(t, found)

	_, err = idx.HasValue(ctx, "", "b1")
	require.ErrorIs(t, ErrEmptyKey, err)

	_, err = idx.HasValue(ctx, "bob", "")
	require.ErrorIs(t, ErrEmptyValue, err)
}

func TestHasAny(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	idx := createIndex(ctx)

	found, err := idx.HasAny(ctx, "nothere")
	require.NoError(t, err)
	require.False(t, found)

	for _, k := range []string{"alice", "bob", ""} {
		found, err := idx.HasAny(ctx, k)
		require.NoError(t, err)
		require.True(t, found)
	}

	require.NoError(t, idx.delKey(ctx, "alice"))
	require.NoError(t, idx.delKey(ctx, "bob"))
	require.NoError(t, idx.delKey(ctx, "cathy"))

	found, err = idx.HasAny(ctx, "")
	require.NoError(t, err)
	require.False(t, found)
}

func TestForEach(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	idx := createIndex(ctx)

	found := map[string]bool{}
	require.NoError(t, idx.forEach(
		ctx,
		"bob",
		func(key, value string) (bool, error) {
			found[value] = true
			return true, nil
		},
	))

	require.Equal(t, 2, len(found))
	for _, value := range []string{"b1", "b2"} {
		require.True(t, found[value])
	}

	values := map[string]string{}
	require.NoError(t, idx.forEach(
		ctx,
		"",
		func(key, value string) (bool, error) {
			values[value] = key
			return true, nil
		},
	))

	require.Equal(t, 4, len(values))
	require.Equal(t, "alice", values["a1"])
	require.Equal(t, "bob", values["b1"])
	require.Equal(t, "bob", values["b2"])
	require.Equal(t, "cathy", values["c1"])
}

func TestSearch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	idx := createIndex(ctx)

	ids, err := idx.Search(ctx, "bob")
	require.NoError(t, err)
	require.Equal(t, 2, len(ids))
	for _, id := range ids {
		require.True(t, id == "b1" || id == "b2")
	}
	require.True(t, ids[0] != ids[1])

	ids, err = idx.Search(ctx, "cathy")
	require.NoError(t, err)
	require.Equal(t, 1, len(ids))
	require.Equal(t, "c1", ids[0])

	ids, err = idx.Search(ctx, "amit")
	require.NoError(t, err)
	require.Equal(t, 0, len(ids))
}
