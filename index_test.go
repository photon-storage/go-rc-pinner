package rcpinner

import (
	"context"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	util "github.com/ipfs/go-ipfs-util"
	mdag "github.com/ipfs/go-merkledag"

	"github.com/photon-storage/go-common/testing/require"
)

var rand = util.NewTimeSeededRand()

func rndNode(t require.TestingTB) *mdag.ProtoNode {
	nd := new(mdag.ProtoNode)
	nd.SetData(make([]byte, 32))
	_, err := io.ReadFull(rand, nd.Data())
	require.NoError(t, err)
	return nd
}

func TestIndex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := ds.NewMapDatastore()
	prefix := ds.NewKey("/data/test_index")
	idx, err := newIndex(ctx, dstore, prefix)
	require.NoError(t, err)

	c1 := rndNode(t).Cid()
	c2 := rndNode(t).Cid()

	cnt, err := idx.get(ctx, c1)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)

	// inc
	total := uint64(0)
	for i := 0; i < 5; i++ {
		cnt, err = idx.inc(ctx, c1, 1)
		require.NoError(t, err)
		require.Equal(t, uint16(i+1), cnt)
		total++
		require.Equal(t, total, idx.totalCount())
		cnt, err = idx.get(ctx, c1)
		require.NoError(t, err)
		require.Equal(t, uint16(i+1), cnt)

		cnt, err = idx.inc(ctx, c2, 1)
		require.NoError(t, err)
		require.Equal(t, uint16(i+1), cnt)
		total++
		require.Equal(t, total, idx.totalCount())
		cnt, err = idx.get(ctx, c2)
		require.NoError(t, err)
		require.Equal(t, uint16(i+1), cnt)
	}
	// dec
	for i := 4; i >= 0; i-- {
		cnt, err = idx.get(ctx, c1)
		require.NoError(t, err)
		require.Equal(t, uint16(i+1), cnt)
		cnt, err = idx.dec(ctx, c1, 1)
		require.NoError(t, err)
		require.Equal(t, uint16(i), cnt)
		total--
		require.Equal(t, total, idx.totalCount())
	}
	cnt, err = idx.dec(ctx, c2, 2)
	require.NoError(t, err)
	require.Equal(t, uint16(3), cnt)
	total -= 2
	require.Equal(t, total, idx.totalCount())

	// get
	cnt, err = idx.get(ctx, c1)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = idx.get(ctx, c2)
	require.NoError(t, err)
	require.Equal(t, uint16(3), cnt)

	cnt, err = idx.dec(ctx, c1, 1)
	require.ErrorIs(t, ds.ErrNotFound, err)
	require.Equal(t, total, idx.totalCount())

	cnt, err = idx.inc(ctx, c1, 2)
	require.NoError(t, err)
	require.Equal(t, uint16(2), cnt)
	total += 2
	require.Equal(t, total, idx.totalCount())

	m := map[cid.Cid]uint16{}
	require.NoError(t, idx.forEach(
		ctx,
		func(k cid.Cid, cnt uint16) (bool, error) {
			m[k] = cnt
			return true, nil
		},
	))
	require.Equal(t, 2, len(m))
	require.Equal(t, uint16(2), m[c1])
	require.Equal(t, uint16(3), m[c2])

	_, err = idx.dec(ctx, c1, 3)
	require.ErrorIs(t, ErrPinCountUnderflow, err)
	require.Equal(t, total, idx.totalCount())
	_, err = idx.inc(ctx, c2, 65533)
	require.ErrorIs(t, ErrPinCountOverflow, err)
	require.Equal(t, total, idx.totalCount())

	idx, err = newIndex(ctx, dstore, prefix)
	require.NoError(t, err)
	require.Equal(t, total, idx.totalCount())
}

func TestIndexUpgrade(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := ds.NewMapDatastore()
	prefix := ds.NewKey("/data/test_index")
	idx, err := newIndex(ctx, dstore, prefix)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = idx.inc(ctx, rndNode(t).Cid(), 1)
		require.NoError(t, err)
	}
	require.Equal(t, uint64(10), idx.totalCount())

	_, err = idx.dstore.Get(ctx, ds.NewKey(totalCountKey))
	require.NoError(t, err)
	require.NoError(t, idx.dstore.Delete(ctx, ds.NewKey(totalCountKey)))

	idx, err = newIndex(ctx, dstore, prefix)
	require.NoError(t, err)
	require.Equal(t, uint64(10), idx.totalCount())
}
