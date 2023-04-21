package rcpinner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"testing"
	"time"

	bs "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	lds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipfspinner "github.com/ipfs/go-ipfs-pinner"
	util "github.com/ipfs/go-ipfs-util"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	mdag "github.com/ipfs/go-merkledag"

	"github.com/photon-storage/go-common/testing/require"
)

var rand = util.NewTimeSeededRand()

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

func randNode() (*mdag.ProtoNode, cid.Cid) {
	nd := new(mdag.ProtoNode)
	nd.SetData(make([]byte, 32))
	_, err := io.ReadFull(rand, nd.Data())
	if err != nil {
		panic(err)
	}
	k := nd.Cid()
	return nd, k
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

	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)

	a, ak := randNode()
	require.NoError(t, dserv.Add(ctx, a))

	// Pin A{}
	require.NoError(t, p.Pin(ctx, a, false))
	assertPinned(t, p, ak)
	assertPinnedWithType(t, p, ak, ipfspinner.Direct)

	// create new node c, to be indirectly pinned through b
	c, _ := randNode()
	require.NoError(t, dserv.Add(ctx, c))
	ck := c.Cid()

	// Create new node b, to be parent to a and c
	b, _ := randNode()
	require.NoError(t, b.AddNodeLink("child_a", a))
	require.NoError(t, b.AddNodeLink("child_c", c))

	require.NoError(t, dserv.Add(ctx, b))
	bk := b.Cid()

	// recursively pin B{A,C}
	require.NoError(t, p.Pin(ctx, b, true))

	assertPinned(t, p, bk)
	assertPinned(t, p, ck)
	assertPinnedWithType(t, p, bk, ipfspinner.Recursive)
	assertPinnedWithType(t, p, ck, ipfspinner.Indirect)

	d, _ := randNode()
	require.NoError(t, d.AddNodeLink("a", a))
	require.NoError(t, d.AddNodeLink("c", c))
	e, _ := randNode()
	require.NoError(t, d.AddNodeLink("e", e))

	// Must be in dagserv for unpin to work
	require.NoError(t, dserv.Add(ctx, e))
	require.NoError(t, dserv.Add(ctx, d))

	// Add D{A,C,E}
	require.NoError(t, p.Pin(ctx, d, true))

	dk := d.Cid()
	assertPinned(t, p, dk)

	cids, err := p.RecursiveKeys(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(cids))
	require.True(t, bk == cids[0] || bk == cids[1])
	require.True(t, dk == cids[0] || dk == cids[1])

	pinned, err := p.CheckIfPinned(ctx, ak, bk, ck, dk)
	require.NoError(t, err)
	require.Equal(t, 4, len(pinned))
	for _, pn := range pinned {
		switch pn.Key {
		case ak:
			require.Equal(t, ipfspinner.Direct, pn.Mode)
		case bk:
			require.Equal(t, ipfspinner.Recursive, pn.Mode)
		case ck:
			require.Equal(t, ipfspinner.Indirect, pn.Mode)
			require.True(t, pn.Via == dk || pn.Via == bk)
		case dk:
			require.Equal(t, ipfspinner.Recursive, pn.Mode)
		}
	}

	cids, err = p.DirectKeys(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(cids))
	require.Equal(t, ak, cids[0])

	cids, _ = p.InternalPins(ctx)
	require.Equal(t, 0, len(cids))

	require.ErrorContains(t,
		"is pinned recursively",
		p.Unpin(ctx, dk, false),
	)

	// Test recursive unpin
	require.NoError(t, p.Unpin(ctx, dk, true))
	require.ErrorIs(t, ipfspinner.ErrNotPinned, p.Unpin(ctx, dk, true))

	require.NoError(t, p.Flush(ctx))

	p, err = New(ctx, dstore, dserv)
	require.NoError(t, err)

	// Test directly pinned
	assertPinned(t, p, ak)
	assertPinnedWithType(t, p, ak, ipfspinner.Direct)
	// Test recursively pinned
	assertPinned(t, p, bk)
	assertPinnedWithType(t, p, bk, ipfspinner.Recursive)

	// Remove the pin but not the index to simulate corruption
	ids, err := p.cidDIdx.Search(ctx, ak.KeyString())
	require.NoError(t, err)
	require.Equal(t, 1, len(ids))
	pp, err := p.loadPin(ctx, ids[0])
	require.NoError(t, err)
	require.Equal(t, ipfspinner.Direct, pp.Mode)
	require.Equal(t, ak, pp.Cid)
	require.NoError(t, p.dstore.Delete(ctx, pp.dsKey()))

	realLog := log
	fakeLog := &fakeLogger{}
	fakeLog.StandardLogger = log
	log = fakeLog
	require.NoError(t, p.Pin(ctx, a, true))
	if fakeLog.lastError == nil {
		t.Error("expected error to be logged")
	} else if fakeLog.lastError.Error() != "found CID index with missing pin" {
		t.Error("did not get expected log message")
	}

	log = realLog
}

func TestAddLoadPin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)

	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)

	a, ak := randNode()
	require.NoError(t, dserv.Add(ctx, a))

	mode := ipfspinner.Recursive
	pid, err := p.addPin(ctx, ak, mode)
	require.NoError(t, err)

	// Load pin and check that data decoded correctly
	pinData, err := p.loadPin(ctx, pid)
	require.NoError(t, err)
	require.Equal(t, mode, pinData.Mode)
	require.Equal(t, ak, pinData.Cid)
}

func TestIsPinnedLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)

	// Create new pinner.  New will not load anything since there are
	// no pins saved in the datastore yet.
	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)

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
			a, _ := randNode()
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
		b, _ := randNode()
		require.NoError(t, b.AddNodeLink("mychild", aNodes[3]))
		// Create C node
		c, _ := randNode()
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

func TestDuplicateSemantics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)

	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)

	a, _ := randNode()
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
	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)
	_, k := randNode()

	require.NoError(t, p.PinWithMode(ctx, k, ipfspinner.Recursive))
	require.NoError(t, p.Flush(ctx))
	assertPinned(t, p, k)
}

func TestPinRecursiveFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)

	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)

	a, _ := randNode()
	b, _ := randNode()
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

func TestLoadDirty(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)

	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)
	require.True(t, p.SetAutosync(false))
	require.False(t, p.SetAutosync(false))
	require.False(t, p.SetAutosync(true))

	a, ak := randNode()
	require.NoError(t, dserv.Add(ctx, a))
	require.NoError(t, p.Pin(ctx, a, true))

	cidAKey := ak.KeyString()
	_, bk := randNode()
	cidBKey := bk.KeyString()

	// Corrupt index
	cidRIndex := p.cidRIdx
	require.NoError(t, cidRIndex.delKey(ctx, cidAKey))
	require.NoError(t, cidRIndex.add(ctx, cidBKey, "not-a-pin-id"))

	// Force dirty, since Pin syncs automatically
	p.setDirty(ctx)

	// Verify dirty
	data, err := dstore.Get(ctx, dirtyKey)
	require.NoError(t, err)
	require.Equal(t, byte(1), data[0])

	has, err := cidRIndex.HasAny(ctx, cidAKey)
	require.NoError(t, err)
	require.False(t, has)

	// Create new pinner on same datastore that was never flushed. This should
	// detect the dirty flag and repair the indexes.
	p, err = New(ctx, dstore, dserv)
	require.NoError(t, err)

	// Verify not dirty
	data, err = dstore.Get(ctx, dirtyKey)
	require.NoError(t, err)
	require.Equal(t, byte(0), data[0])

	// Verify index rebuilt
	cidRIndex = p.cidRIdx
	has, err = cidRIndex.HasAny(ctx, cidAKey)
	require.NoError(t, err)
	require.True(t, has)

	removed, err := p.removePinsForCid(ctx, bk, ipfspinner.Any)
	require.NoError(t, err)
	require.True(t, removed)
}

func TestEncodeDecodePin(t *testing.T) {
	_, c := randNode()

	pin := newPin(c, ipfspinner.Recursive)
	pin.Metadata = make(map[string]interface{}, 2)
	pin.Metadata["hello"] = "world"
	pin.Metadata["foo"] = "bar"

	encBytes, err := encodePin(pin)
	require.NoError(t, err)

	decPin, err := decodePin(pin.Id, encBytes)
	require.NoError(t, err)

	require.Equal(t, pin.Id, decPin.Id)
	require.Equal(t, pin.Cid, decPin.Cid)
	require.Equal(t, pin.Mode, decPin.Mode)
	for key, val := range pin.Metadata {
		dval, ok := decPin.Metadata[key]
		require.True(t, ok)
		require.Equal(t, val.(string), dval.(string))
	}
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
		n, _ := randNode()
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

// BenchmarkLoadRebuild loads a pinner that has some number of saved pins, and
// compares the load time when rebuilding indexes to loading without rebuilding
// indexes.
func BenchmarkLoad(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore, dserv := makeStore(b)
	pinner, err := New(ctx, dstore, dserv)
	require.NoError(b, err)

	nodes := makeNodes(b, 4096, dserv)
	pinNodes(b, nodes, pinner, true)

	b.Run("RebuildTrue", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			require.NoError(b, dstore.Put(ctx, dirtyKey, []byte{1}))

			_, err := New(ctx, dstore, dserv)
			require.NoError(b, err)
		}
	})

	b.Run("RebuildFalse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			require.NoError(b, dstore.Put(ctx, dirtyKey, []byte{0}))
			_, err := New(ctx, dstore, dserv)
			require.NoError(b, err)
		}
	})
}

// BenchmarkNthPins shows the time it takes to create/save 1 pin when a number
// of other pins already exist.  Each run in the series shows performance for
// creating a pin in a larger number of existing pins.
func BenchmarkNthPin(b *testing.B) {
	dstore, dserv := makeStore(b)
	pinner, err := New(context.Background(), dstore, dserv)
	require.NoError(b, err)

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
			pinner, err := New(context.Background(), dstore, dserv)
			require.NoError(b, err)
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
			pinner, err := New(context.Background(), dstore, dserv)
			require.NoError(b, err)
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
			pinner, err := New(context.Background(), dstore, dserv)
			require.NoError(b, err)
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

func BenchmarkRebuild(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore, dserv := makeStore(b)
	pinIncr := 32768

	for pins := pinIncr; pins <= pinIncr*5; pins += pinIncr {
		pinner, err := New(ctx, dstore, dserv)
		require.NoError(b, err)
		nodes := makeNodes(b, pinIncr, dserv)
		pinNodes(b, nodes, pinner, true)

		b.Run(fmt.Sprintf("Rebuild %d", pins), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				require.NoError(b, dstore.Put(ctx, dirtyKey, []byte{1}))
				_, err = New(ctx, dstore, dserv)
				require.NoError(b, err)
			}
		})
	}
}

func TestCidIndex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore, dserv := makeStore(t)
	pinner, err := New(ctx, dstore, dserv)
	require.NoError(t, err)
	nodes := makeNodes(t, 1, dserv)
	node := nodes[0]

	c := node.Cid()
	cidKey := c.KeyString()

	// Pin the cid
	pid, err := pinner.addPin(ctx, c, ipfspinner.Recursive)
	require.NoError(t, err)

	t.Log("Added pin:", pid)
	t.Log("CID index:", c.String(), "-->", pid)

	// Check that the index exists
	has, err := pinner.cidRIdx.HasAny(ctx, cidKey)
	require.NoError(t, err)
	require.True(t, has)

	// Check that searching for the cid returns a value
	values, err := pinner.cidRIdx.Search(ctx, cidKey)
	require.NoError(t, err)
	require.Equal(t, 1, len(values))
	require.Equal(t, pid, values[0])

	// Check that index has specific value
	has, err = pinner.cidRIdx.HasValue(ctx, cidKey, pid)
	require.NoError(t, err)
	require.True(t, has)

	// Iterate values of index
	var seen bool
	require.NoError(t, pinner.cidRIdx.forEach(
		ctx,
		"",
		func(key, value string) (bool, error) {
			require.False(t, seen)
			require.Equal(t, cidKey, key)
			require.Equal(t, pid, value)
			seen = true
			return true, nil
		},
	))

	// Load all pins from the datastore.
	q := query.Query{
		Prefix: pinKeyPath,
	}
	results, err := pinner.dstore.Query(ctx, q)
	require.NoError(t, err)
	defer results.Close()

	// Iterate all pins and check if the corresponding recursive or direct
	// index is missing.  If the index is missing then create the index.
	seen = false
	for r := range results.Next() {
		require.False(t, seen)
		require.NoError(t, r.Error)
		ent := r.Entry
		pp, err := decodePin(path.Base(ent.Key), ent.Value)
		require.NoError(t, err)
		t.Log("Found pin:", pp.Id)
		require.Equal(t, pid, pp.Id)
		seen = true
	}
}

func TestRebuild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore, dserv := makeStore(t)
	pinner, err := New(ctx, dstore, dserv)
	require.NoError(t, err)
	nodes := makeNodes(t, 3, dserv)
	pinNodes(t, nodes, pinner, true)

	c1 := nodes[0].Cid()
	cid1Key := c1.KeyString()
	c2 := nodes[1].Cid()
	cid2Key := c2.KeyString()
	c3 := nodes[2].Cid()
	cid3Key := c3.KeyString()

	// Get pin IDs
	values, err := pinner.cidRIdx.Search(ctx, cid1Key)
	require.NoError(t, err)
	pid1 := values[0]
	values, err = pinner.cidRIdx.Search(ctx, cid2Key)
	require.NoError(t, err)
	pid2 := values[0]
	values, err = pinner.cidRIdx.Search(ctx, cid3Key)
	require.NoError(t, err)
	pid3 := values[0]

	// Corrupt by adding direct index when there is already a recursive index
	require.NoError(t, pinner.cidDIdx.add(ctx, cid1Key, pid1))

	// Corrupt index by deleting cid index 2 to simulate an incomplete
	// add or delete
	require.NoError(t, pinner.cidRIdx.delKey(ctx, cid2Key))

	// Corrupt index by deleting pin to simulate corruption
	pp, err := pinner.loadPin(ctx, pid3)
	require.NoError(t, err)
	require.NoError(t, pinner.dstore.Delete(ctx, pp.dsKey()))

	pinner.setDirty(ctx)

	// Rebuild indexes
	pinner, err = New(ctx, dstore, dserv)
	require.NoError(t, err)

	// Verify that indexes have same values as before
	verifyIndexValue(t, ctx, pinner, cid1Key, pid1)
	verifyIndexValue(t, ctx, pinner, cid2Key, pid2)
	verifyIndexValue(t, ctx, pinner, cid3Key, pid3)
}

func verifyIndexValue(
	t *testing.T,
	ctx context.Context,
	pinner *pinner,
	cidKey string,
	expectedPid string,
) {
	values, err := pinner.cidRIdx.Search(ctx, cidKey)
	require.NoError(t, err)
	require.Equal(t, 1, len(values))
	require.Equal(t, expectedPid, values[0])
	has, err := pinner.cidDIdx.HasAny(ctx, cidKey)
	require.NoError(t, err)
	require.False(t, has)
}
