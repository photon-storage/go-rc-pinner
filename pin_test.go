package rcpinner

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	bs "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	lds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipfspinner "github.com/ipfs/go-ipfs-pinner"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	mdag "github.com/ipfs/go-merkledag"

	"github.com/photon-storage/go-common/testing/require"
)

type fakeLogger struct {
	logging.StandardLogger
	lastError error
}

func (f *fakeLogger) Error(args ...interface{}) {
	f.lastError = errors.New(fmt.Sprint(args...))
}

func (f *fakeLogger) Errorf(format string, args ...interface{}) {
	f.lastError = fmt.Errorf(format, args...)
}

func assertPinned(t *testing.T, p ipfspinner.Pinner, c cid.Cid) {
	_, pinned, err := p.IsPinned(context.Background(), c)
	require.NoError(t, err)
	require.True(t, pinned)
}

func assertPinnedWithType(
	t *testing.T,
	p ipfspinner.Pinner,
	c cid.Cid,
	mode ipfspinner.Mode,
) {
	modeText, pinned, err := p.IsPinnedWithType(context.Background(), c, mode)
	require.NoError(t, err)
	require.True(t, pinned)

	if mode == ipfspinner.Any || mode == ipfspinner.Indirect {
		return
	}

	expect, ok := ipfspinner.ModeToString(mode)
	require.True(t, ok)
	require.Equal(t, expect, modeText)
}

func assertUnpinned(t *testing.T, p ipfspinner.Pinner, c cid.Cid) {
	_, pinned, err := p.IsPinned(context.Background(), c)
	require.NoError(t, err)
	require.False(t, pinned)
}

func TestPinnerBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	p := New(ctx, dstore, dserv)

	a := rndNode(t)
	require.NoError(t, dserv.Add(ctx, a))

	// Pin A{}
	require.NoError(t, p.Pin(ctx, a, false))
	assertPinned(t, p, a.Cid())
	assertPinnedWithType(t, p, a.Cid(), ipfspinner.Direct)

	// create new node c, to be indirectly pinned through b
	c := rndNode(t)
	require.NoError(t, dserv.Add(ctx, c))

	// Create new node b, to be parent to a and c
	b := rndNode(t)
	require.NoError(t, b.AddNodeLink("child_a", a))
	require.NoError(t, b.AddNodeLink("child_c", c))

	require.NoError(t, dserv.Add(ctx, b))

	// recursively pin B{A,C}
	require.NoError(t, p.Pin(ctx, b, true))

	assertPinned(t, p, b.Cid())
	assertPinned(t, p, c.Cid())

	assertPinnedWithType(t, p, b.Cid(), ipfspinner.Recursive)
	assertPinnedWithType(t, p, c.Cid(), ipfspinner.Indirect)

	d := rndNode(t)
	require.NoError(t, d.AddNodeLink("a", a))
	require.NoError(t, d.AddNodeLink("c", c))
	e := rndNode(t)
	require.NoError(t, d.AddNodeLink("e", e))

	// Must be in dagserv for unpin to work
	require.NoError(t, dserv.Add(ctx, e))
	require.NoError(t, dserv.Add(ctx, d))

	// Add D{A,C,E}
	require.NoError(t, p.Pin(ctx, d, true))

	assertPinned(t, p, d.Cid())

	cids, err := p.RecursiveKeys(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(cids))
	require.True(t, b.Cid() == cids[0] || b.Cid() == cids[1])
	require.True(t, d.Cid() == cids[0] || d.Cid() == cids[1])

	pinned, err := p.CheckIfPinned(ctx, a.Cid(), b.Cid(), c.Cid(), d.Cid())
	require.NoError(t, err)
	require.Equal(t, 4, len(pinned))
	for _, pn := range pinned {
		switch pn.Key {
		case a.Cid():
			require.Equal(t, ipfspinner.Direct, pn.Mode)
		case b.Cid():
			require.Equal(t, ipfspinner.Recursive, pn.Mode)
		case c.Cid():
			require.Equal(t, ipfspinner.Indirect, pn.Mode)
			require.True(t, pn.Via == d.Cid() || pn.Via == b.Cid())
		case d.Cid():
			require.Equal(t, ipfspinner.Recursive, pn.Mode)
		}
	}

	cids, err = p.DirectKeys(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(cids))
	require.Equal(t, a.Cid(), cids[0])

	cids, _ = p.InternalPins(ctx)
	require.Equal(t, 0, len(cids))

	require.ErrorContains(t,
		"is pinned recursively",
		p.Unpin(ctx, d.Cid(), false),
	)

	// Test recursive unpin
	require.NoError(t, p.Unpin(ctx, d.Cid(), true))
	require.ErrorIs(t, ipfspinner.ErrNotPinned, p.Unpin(ctx, d.Cid(), true))

	require.NoError(t, p.Flush(ctx))

	p = New(ctx, dstore, dserv)

	// Test directly pinned
	assertPinned(t, p, a.Cid())
	assertPinnedWithType(t, p, a.Cid(), ipfspinner.Direct)
	// Test recursively pinned
	assertPinned(t, p, b.Cid())
	assertPinnedWithType(t, p, b.Cid(), ipfspinner.Recursive)
}

func TestIsPinnedLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	p := New(ctx, dstore, dserv)

	makeTree := func(
		ctx context.Context,
		aBranchLen int,
		dserv ipld.DAGService,
		p ipfspinner.Pinner,
	) ([]cid.Cid, cid.Cid, cid.Cid) {
		require.True(t, aBranchLen >= 3)

		aNodes := make([]*mdag.ProtoNode, aBranchLen)
		aKeys := make([]cid.Cid, aBranchLen)
		for i := 0; i < aBranchLen; i++ {
			a := rndNode(t)
			if i >= 1 {
				require.NoError(t, a.AddNodeLink("child", aNodes[i-1]))
			}
			require.NoError(t, dserv.Add(ctx, a))
			aNodes[i] = a
			aKeys[i] = a.Cid()
		}
		// Pin last A recursively
		require.NoError(t, p.Pin(ctx, aNodes[aBranchLen-1], true))

		// Create node B and add A3 as child
		b := rndNode(t)
		require.NoError(t, b.AddNodeLink("mychild", aNodes[3]))
		// Create C node
		c := rndNode(t)
		// Add A0 as child of C
		require.NoError(t, c.AddNodeLink("child", aNodes[0]))
		// Add C
		require.NoError(t, dserv.Add(ctx, c))
		// Add C to B and Add B
		require.NoError(t, b.AddNodeLink("myotherchild", c))
		require.NoError(t, dserv.Add(ctx, b))
		// Pin C recursively
		require.NoError(t, p.Pin(ctx, c, true))
		// Pin B recursively
		require.NoError(t, p.Pin(ctx, b, true))

		require.NoError(t, p.Flush(ctx))

		return aKeys, b.Cid(), c.Cid()
	}
	// Test that lookups work in pins which share
	// the same branches.  For that construct this tree:
	//
	// A5->A4->A3->A2->A1->A0
	//         /           /
	// B-------           /
	//  \                /
	//   C---------------
	//
	// This ensures that IsPinned works for all objects both when they
	// are pinned and once they have been unpinned.
	aKeys, bk, ck := makeTree(ctx, 6, dserv, p)

	for i := 0; i < 6; i++ {
		assertPinned(t, p, aKeys[i])
	}
	assertPinned(t, p, ck)
	assertPinned(t, p, bk)

	// Unpin A5 recursively
	require.NoError(t, p.Unpin(ctx, aKeys[5], true))

	for i := 0; i < 4; i++ {
		assertPinned(t, p, aKeys[i])
	}
	assertUnpinned(t, p, aKeys[4])
	assertUnpinned(t, p, aKeys[5])

	// Unpin B recursively
	require.NoError(t, p.Unpin(ctx, bk, true))
	assertUnpinned(t, p, bk)
	assertUnpinned(t, p, aKeys[1])
	assertUnpinned(t, p, aKeys[2])
	assertUnpinned(t, p, aKeys[3])
	assertPinned(t, p, aKeys[0])
	assertPinned(t, p, ck)
}

func TestDuplicatedPins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	p := New(ctx, dstore, dserv)

	//   A    E
	//  / \  /
	// B   C
	//      \
	//       D
	a := rndNode(t)
	b := rndNode(t)
	c := rndNode(t)
	d := rndNode(t)
	e := rndNode(t)
	require.NoError(t, c.AddNodeLink("child_d", d))
	require.NoError(t, a.AddNodeLink("child_b", b))
	require.NoError(t, a.AddNodeLink("child_c", c))
	require.NoError(t, e.AddNodeLink("child_c", c))
	require.NoError(t, dserv.Add(ctx, a))
	require.NoError(t, dserv.Add(ctx, b))
	require.NoError(t, dserv.Add(ctx, c))
	require.NoError(t, dserv.Add(ctx, d))
	require.NoError(t, dserv.Add(ctx, e))

	// a=3,c=1,e=1
	require.NoError(t, p.Pin(ctx, c, false))
	assertPinnedWithType(t, p, c.Cid(), ipfspinner.Direct)
	require.NoError(t, p.Pin(ctx, a, false))
	assertPinnedWithType(t, p, a.Cid(), ipfspinner.Direct)
	require.NoError(t, p.Pin(ctx, a, true))
	assertPinnedWithType(t, p, a.Cid(), ipfspinner.Recursive)
	require.NoError(t, p.Pin(ctx, e, true))
	assertPinnedWithType(t, p, e.Cid(), ipfspinner.Recursive)
	require.NoError(t, p.Pin(ctx, a, true))
	assertPinnedWithType(t, p, a.Cid(), ipfspinner.Recursive)

	require.ErrorContains(t,
		"is pinned recursively",
		p.Unpin(ctx, a.Cid(), false),
	)
	require.NoError(t, p.Unpin(ctx, a.Cid(), true))
	assertPinnedWithType(t, p, a.Cid(), ipfspinner.Recursive)
	require.NoError(t, p.Unpin(ctx, a.Cid(), true))
	assertPinnedWithType(t, p, a.Cid(), ipfspinner.Recursive)
	require.NoError(t, p.Unpin(ctx, a.Cid(), true))
	assertUnpinned(t, p, a.Cid())
	assertUnpinned(t, p, b.Cid())
	assertPinnedWithType(t, p, c.Cid(), ipfspinner.Direct)
	require.NoError(t, p.Unpin(ctx, c.Cid(), false))
	assertPinnedWithType(t, p, c.Cid(), ipfspinner.Indirect)
	require.NoError(t, p.Unpin(ctx, e.Cid(), true))
	assertUnpinned(t, p, e.Cid())
	assertUnpinned(t, p, c.Cid())
	assertUnpinned(t, p, d.Cid())
}

func TestPinModeConflictSemantics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	p := New(ctx, dstore, dserv)

	a := rndNode(t)
	require.NoError(t, dserv.Add(ctx, a))

	// pin is recursively
	require.NoError(t, p.Pin(ctx, a, true))

	// pinning directly should fail
	require.ErrorContains(t,
		"already pinned recursively",
		p.Pin(ctx, a, false),
	)

	// pinning recursively again should succeed
	require.NoError(t, p.Pin(ctx, a, true))
}

func TestFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	p := New(ctx, dstore, dserv)

	c := rndNode(t).Cid()
	require.NoError(t, p.PinWithMode(ctx, c, ipfspinner.Recursive))
	require.NoError(t, p.Flush(ctx))
	assertPinned(t, p, c)
}

func TestPinRecursiveFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	p := New(ctx, dstore, dserv)

	a := rndNode(t)
	b := rndNode(t)
	require.NoError(t, a.AddNodeLink("child", b))

	// NOTE: This isnt a time based test, we expect the pin to fail
	mctx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()

	require.NotNil(t, p.Pin(mctx, a, true))
	require.NoError(t, dserv.Add(ctx, b))
	require.NoError(t, dserv.Add(ctx, a))

	// this one is time based... but shouldnt cause any issues
	mctx, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()
	require.NoError(t, p.Pin(mctx, a, true))
}

func TestCidIndex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore, dserv := makeStore(t)
	pinner := New(ctx, dstore, dserv)
	nodes := makeNodes(t, 1, dserv)
	node := nodes[0]

	// Pin the cid
	require.NoError(t, pinner.Pin(ctx, node, true))

	t.Log("Added pin:", node.Cid().String())

	// Check that the index exists
	cnt, err := pinner.cidRIdx.get(ctx, node.Cid())
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)

	// Iterate values of index
	var seen bool
	require.NoError(t, pinner.cidRIdx.forEach(
		ctx,
		func(c cid.Cid, cnt uint16) (bool, error) {
			require.False(t, seen)
			require.Equal(t, node.Cid(), c)
			require.Equal(t, uint16(1), cnt)
			seen = true
			return true, nil
		},
	))
}

func makeNodes(
	t require.TestingTB,
	count int,
	dserv ipld.DAGService,
) []ipld.Node {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := make([]ipld.Node, count)
	for i := 0; i < count; i++ {
		n := rndNode(t)
		require.NoError(t, dserv.Add(ctx, n))
		nodes[i] = n
	}
	return nodes
}

func pinNodes(
	t require.TestingTB,
	nodes []ipld.Node,
	p ipfspinner.Pinner,
	recursive bool,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := range nodes {
		require.NoError(t, p.Pin(ctx, nodes[i], recursive))
	}
	require.NoError(t, p.Flush(ctx))
}

func unpinNodes(
	t require.TestingTB,
	nodes []ipld.Node,
	p ipfspinner.Pinner,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := range nodes {
		require.NoError(t, p.Unpin(ctx, nodes[i].Cid(), true))
	}
	require.NoError(t, p.Flush(ctx))
}

type batchWrap struct {
	ds.Datastore
}

func (d *batchWrap) Batch(_ context.Context) (ds.Batch, error) {
	return ds.NewBasicBatch(d), nil
}

func makeStore(t require.TestingTB) (ds.Datastore, ipld.DAGService) {
	ldstore, err := lds.NewDatastore("", nil)
	require.NoError(t, err)
	dstore := &batchWrap{ldstore}
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	return dstore, mdag.NewDAGService(bserv)
}

// BenchmarkNthPins shows the time it takes to create/save 1 pin when a number
// of other pins already exist.  Each run in the series shows performance for
// creating a pin in a larger number of existing pins.
func BenchmarkNthPin(b *testing.B) {
	dstore, dserv := makeStore(b)
	pinner := New(context.Background(), dstore, dserv)

	for count := 1000; count <= 10000; count += 1000 {
		b.Run(fmt.Sprint("PinDS-", count), func(b *testing.B) {
			benchmarkNthPin(b, count, pinner, dserv)
		})
	}
}

func benchmarkNthPin(
	b *testing.B,
	count int,
	pinner ipfspinner.Pinner,
	dserv ipld.DAGService,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := makeNodes(b, count, dserv)
	pinNodes(b, nodes[:count-1], pinner, true)
	b.ResetTimer()

	which := count - 1
	for i := 0; i < b.N; i++ {
		// Pin the Nth node and Flush
		require.NoError(b, pinner.Pin(ctx, nodes[which], true))
		require.NoError(b, pinner.Flush(ctx))
		// Unpin the nodes so that it can pinned next iter.
		b.StopTimer()
		require.NoError(b, pinner.Unpin(ctx, nodes[which].Cid(), true))
		require.NoError(b, pinner.Flush(ctx))
		b.StartTimer()
	}
}

// BenchmarkNPins demonstrates creating individual pins.  Each run in the
// series shows performance for a larger number of individual pins.
func BenchmarkNPins(b *testing.B) {
	for count := 128; count < 16386; count <<= 1 {
		b.Run(fmt.Sprint("PinDS-", count), func(b *testing.B) {
			dstore, dserv := makeStore(b)
			pinner := New(context.Background(), dstore, dserv)
			benchmarkNPins(b, count, pinner, dserv)
		})
	}
}

func benchmarkNPins(
	b *testing.B,
	count int,
	pinner ipfspinner.Pinner,
	dserv ipld.DAGService,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := makeNodes(b, count, dserv)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Pin all the nodes one at a time.
		for j := range nodes {
			require.NoError(b, pinner.Pin(ctx, nodes[j], true))
			require.NoError(b, pinner.Flush(ctx))
		}

		// Unpin all nodes so that they can be pinned next iter.
		b.StopTimer()
		unpinNodes(b, nodes, pinner)
		b.StartTimer()
	}
}

// BenchmarkNUnpins demonstrates unpinning individual pins. Each run in the
// series shows performance for a larger number of individual unpins.
func BenchmarkNUnpins(b *testing.B) {
	for count := 128; count < 16386; count <<= 1 {
		b.Run(fmt.Sprint("UnpinDS-", count), func(b *testing.B) {
			dstore, dserv := makeStore(b)
			pinner := New(context.Background(), dstore, dserv)
			benchmarkNUnpins(b, count, pinner, dserv)
		})
	}
}

func benchmarkNUnpins(
	b *testing.B,
	count int,
	pinner ipfspinner.Pinner,
	dserv ipld.DAGService,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := makeNodes(b, count, dserv)
	pinNodes(b, nodes, pinner, true)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range nodes {
			// Unpin nodes one at a time.
			require.NoError(b, pinner.Unpin(ctx, nodes[j].Cid(), true))
			require.NoError(b, pinner.Flush(ctx))
		}
		// Pin all nodes so that they can be unpinned next iter.
		b.StopTimer()
		pinNodes(b, nodes, pinner, true)
		b.StartTimer()
	}
}

// BenchmarkPinAllSeries shows times to pin all nodes with only one Flush at
// the end.
func BenchmarkPinAll(b *testing.B) {
	for count := 128; count < 16386; count <<= 1 {
		b.Run(fmt.Sprint("PinAllDS-", count), func(b *testing.B) {
			dstore, dserv := makeStore(b)
			pinner := New(context.Background(), dstore, dserv)
			benchmarkPinAll(b, count, pinner, dserv)
		})
	}
}

func benchmarkPinAll(
	b *testing.B,
	count int,
	pinner ipfspinner.Pinner,
	dserv ipld.DAGService,
) {
	nodes := makeNodes(b, count, dserv)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pinNodes(b, nodes, pinner, true)

		b.StopTimer()
		unpinNodes(b, nodes, pinner)
		b.StartTimer()
	}
}
