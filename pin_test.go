package rcpinner

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	bs "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	mdag "github.com/ipfs/boxo/ipld/merkledag"
	pin "github.com/ipfs/boxo/pinning/pinner"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	lds "github.com/ipfs/go-ds-leveldb"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	"go.uber.org/atomic"

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

func assertPinned(t *testing.T, p pin.Pinner, c cid.Cid) {
	_, pinned, err := p.IsPinned(context.Background(), c)
	require.NoError(t, err)
	require.True(t, pinned)
}

func assertPinnedWithType(
	t *testing.T,
	p pin.Pinner,
	c cid.Cid,
	mode pin.Mode,
) {
	modeText, pinned, err := p.IsPinnedWithType(context.Background(), c, mode)
	require.NoError(t, err)
	require.True(t, pinned)

	if mode == pin.Any || mode == pin.Indirect {
		return
	}

	expect, ok := pin.ModeToString(mode)
	require.True(t, ok)
	require.Equal(t, expect, modeText)
}

func assertUnpinned(t *testing.T, p pin.Pinner, c cid.Cid) {
	_, pinned, err := p.IsPinned(context.Background(), c)
	require.NoError(t, err)
	require.False(t, pinned)
}

func TestPinnerBasics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)

	a := rndNode(t)
	require.NoError(t, dserv.Add(ctx, a))

	// Pin A{}
	cnt, err := p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	require.NoError(t, p.Pin(ctx, a, true))
	assertPinned(t, p, a.Cid())
	assertPinnedWithType(t, p, a.Cid(), pin.Recursive)
	cnt, err = p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)

	// create new node c, to be indirectly pinned through b
	c := rndNode(t)
	require.NoError(t, dserv.Add(ctx, c))

	// Create new node b, to be parent to a and c
	b := rndNode(t)
	require.NoError(t, b.AddNodeLink("child_a", a))
	require.NoError(t, b.AddNodeLink("child_c", c))

	require.NoError(t, dserv.Add(ctx, b))

	// recursively pin B{A,C}
	cnt, err = p.GetCount(ctx, b.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, c.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	require.NoError(t, p.Pin(ctx, b, true))
	assertPinned(t, p, b.Cid())
	assertPinnedWithType(t, p, b.Cid(), pin.Recursive)
	cnt, err = p.GetCount(ctx, b.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	assertPinned(t, p, c.Cid())
	assertPinnedWithType(t, p, c.Cid(), pin.Indirect)
	cnt, err = p.GetCount(ctx, c.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)

	d := rndNode(t)
	require.NoError(t, d.AddNodeLink("a", a))
	require.NoError(t, d.AddNodeLink("c", c))
	e := rndNode(t)
	require.NoError(t, d.AddNodeLink("e", e))

	// Must be in dagserv for unpin to work
	require.NoError(t, dserv.Add(ctx, e))
	require.NoError(t, dserv.Add(ctx, d))

	// Add D{A,C,E}
	cnt, err = p.GetCount(ctx, d.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	require.NoError(t, p.Pin(ctx, d, true))
	assertPinned(t, p, d.Cid())
	cnt, err = p.GetCount(ctx, d.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)

	// Add direct pin A, D, F
	// Direct pins and recursive pins are independent
	f := rndNode(t)
	cnt, err = p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	cnt, err = p.GetCount(ctx, d.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	cnt, err = p.GetCount(ctx, f.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, a.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, d.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, f.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	require.NoError(t, p.Pin(ctx, a, false))
	require.NoError(t, p.Pin(ctx, d, false))
	require.NoError(t, p.Pin(ctx, f, false))
	cnt, err = p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	cnt, err = p.GetCount(ctx, d.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	cnt, err = p.GetCount(ctx, f.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, a.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	cnt, err = p.GetCount(ctx, d.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	cnt, err = p.GetCount(ctx, f.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)

	cids := readCh(t, p.RecursiveKeys(ctx))
	require.Equal(t, 3, len(cids))
	require.True(t, cids[a.Cid()])
	require.True(t, cids[b.Cid()])
	require.True(t, cids[d.Cid()])

	// DAG: D{A,C,E}, B{A,C}
	// Recursive: A,B,D
	// Direct: A,D,F
	pinned, err := p.CheckIfPinned(ctx, a.Cid(), b.Cid(), c.Cid(), d.Cid(), e.Cid(), f.Cid())
	require.NoError(t, err)
	require.Equal(t, 6, len(pinned))
	for _, pn := range pinned {
		if pn.Key == c.Cid() || pn.Key == e.Cid() {
			require.Equal(t, pin.Indirect, pn.Mode)
			require.True(t, pn.Via == d.Cid() || pn.Via == b.Cid())
		} else if pn.Key == a.Cid() || pn.Key == d.Cid() {
			require.Equal(t, pin.Any, pn.Mode)
		} else if pn.Key == f.Cid() {
			require.Equal(t, pin.Direct, pn.Mode)
		} else {
			require.Equal(t, pin.Recursive, pn.Mode)
		}
	}

	cids = readCh(t, p.DirectKeys(ctx))
	require.NoError(t, err)
	require.Equal(t, 3, len(cids))
	require.True(t, cids[a.Cid()])
	require.True(t, cids[d.Cid()])
	require.True(t, cids[f.Cid()])

	cids = readCh(t, p.InternalPins(ctx))
	require.NoError(t, err)
	require.Equal(t, 0, len(cids))

	// Direct unpin
	require.ErrorIs(t, pin.ErrNotPinned, p.Unpin(ctx, c.Cid(), false))
	require.NoError(t, p.Unpin(ctx, d.Cid(), false))
	require.NoError(t, p.Unpin(ctx, f.Cid(), false))

	// Recursive unpin
	require.ErrorIs(t, pin.ErrNotPinned, p.Unpin(ctx, c.Cid(), true))
	require.NoError(t, p.Unpin(ctx, d.Cid(), true))
	require.ErrorIs(t, pin.ErrNotPinned, p.Unpin(ctx, d.Cid(), true))

	require.NoError(t, p.Flush(ctx))

	p, err = New(ctx, dstore, dserv)
	require.NoError(t, err)

	// Test recursively pinned
	assertPinned(t, p, a.Cid())
	assertPinnedWithType(t, p, a.Cid(), pin.Recursive)
	assertPinned(t, p, b.Cid())
	assertPinnedWithType(t, p, b.Cid(), pin.Recursive)

	// Make ref count greater than 1
	require.NoError(t, p.Pin(ctx, a, true))
	require.NoError(t, p.Pin(ctx, b, true))

	cnt, err = p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(2), cnt)
	cnt, err = p.GetCount(ctx, b.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(2), cnt)
	cnt, err = p.GetCount(ctx, c.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, d.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, e.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, f.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)

	cnt, err = p.GetCount(ctx, a.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	cnt, err = p.GetCount(ctx, b.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, c.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, d.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, e.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, f.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
}

func TestIsPinnedLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)

	makeTree := func(
		ctx context.Context,
		aBranchLen int,
		dserv ipld.DAGService,
		p pin.Pinner,
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
	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)

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

	// a=2,c=1,e=1
	require.NoError(t, p.Pin(ctx, a, true))
	assertPinnedWithType(t, p, a.Cid(), pin.Recursive)
	require.NoError(t, p.Pin(ctx, c, true))
	assertPinnedWithType(t, p, c.Cid(), pin.Recursive)
	require.NoError(t, p.Pin(ctx, e, true))
	assertPinnedWithType(t, p, e.Cid(), pin.Recursive)
	require.NoError(t, p.Pin(ctx, a, true))
	assertPinnedWithType(t, p, a.Cid(), pin.Recursive)
	assertPinnedWithType(t, p, b.Cid(), pin.Indirect)
	assertPinnedWithType(t, p, d.Cid(), pin.Indirect)

	require.NoError(t, p.Unpin(ctx, a.Cid(), true))
	assertPinnedWithType(t, p, a.Cid(), pin.Recursive)
	assertPinnedWithType(t, p, b.Cid(), pin.Indirect)
	require.NoError(t, p.Unpin(ctx, a.Cid(), true))
	assertUnpinned(t, p, a.Cid())
	assertUnpinned(t, p, b.Cid())
	require.ErrorIs(t, pin.ErrNotPinned, p.Unpin(ctx, a.Cid(), true))

	assertPinnedWithType(t, p, c.Cid(), pin.Recursive)
	require.NoError(t, p.Unpin(ctx, c.Cid(), true))
	assertPinnedWithType(t, p, c.Cid(), pin.Indirect)

	require.NoError(t, p.Unpin(ctx, e.Cid(), true))
	assertUnpinned(t, p, e.Cid())
	assertUnpinned(t, p, c.Cid())
	assertUnpinned(t, p, d.Cid())
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

	nd := rndNode(t)
	require.NoError(t, dserv.Add(ctx, nd))
	require.NoError(t, p.PinWithMode(ctx, nd.Cid(), pin.Recursive))
	require.NoError(t, p.Flush(ctx))
	assertPinned(t, p, nd.Cid())
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

func TestRefCount(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)

	a := rndNode(t)
	require.NoError(t, dserv.Add(ctx, a))

	cnt, err := p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, a.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)

	require.NoError(t, p.Pin(ctx, a, true))
	require.NoError(t, p.Pin(ctx, a, false))
	require.NoError(t, p.Pin(ctx, a, false))

	cnt, err = p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	cnt, err = p.GetCount(ctx, a.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(2), cnt)

	require.NoError(t, p.IncCount(ctx, a.Cid(), true))
	require.NoError(t, p.IncCount(ctx, a.Cid(), false))

	cnt, err = p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(2), cnt)
	cnt, err = p.GetCount(ctx, a.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(3), cnt)

	require.NoError(t, p.DecCount(ctx, a.Cid(), true))
	require.NoError(t, p.DecCount(ctx, a.Cid(), false))

	cnt, err = p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	cnt, err = p.GetCount(ctx, a.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(2), cnt)

	require.NoError(t, p.Unpin(ctx, a.Cid(), true))
	require.NoError(t, p.Unpin(ctx, a.Cid(), false))

	cnt, err = p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, a.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
}

func TestRefCountUpdates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)

	a := rndNode(t)
	b := rndNode(t)
	c := rndNode(t)
	require.NoError(t, dserv.Add(ctx, a))
	require.NoError(t, dserv.Add(ctx, b))
	require.NoError(t, dserv.Add(ctx, c))

	cnt, err := p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, b.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, c.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)

	require.NoError(t, p.UpdateCounts(
		ctx,
		[]*UpdateCount{
			&UpdateCount{
				CID:       a.Cid(),
				Recursive: true,
			},
			&UpdateCount{
				CID:       b.Cid(),
				Recursive: true,
			},
			&UpdateCount{
				CID:       c.Cid(),
				Recursive: false,
			},
		},
		nil,
	))

	cnt, err = p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	cnt, err = p.GetCount(ctx, b.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	cnt, err = p.GetCount(ctx, c.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)

	require.ErrorIs(t, pin.ErrNotPinned, p.UpdateCounts(
		ctx,
		[]*UpdateCount{
			&UpdateCount{
				CID:       a.Cid(),
				Recursive: true,
			},
			&UpdateCount{
				CID:       b.Cid(),
				Recursive: true,
			},
		},
		[]*UpdateCount{
			&UpdateCount{
				CID:       c.Cid(),
				Recursive: true,
			},
		},
	))

	cnt, err = p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	cnt, err = p.GetCount(ctx, b.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	cnt, err = p.GetCount(ctx, c.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)

	require.NoError(t, p.UpdateCounts(
		ctx,
		[]*UpdateCount{
			&UpdateCount{
				CID:       b.Cid(),
				Recursive: true,
			},
		},
		[]*UpdateCount{
			&UpdateCount{
				CID:       a.Cid(),
				Recursive: true,
			},
			&UpdateCount{
				CID:       c.Cid(),
				Recursive: false,
			},
		},
	))

	cnt, err = p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
	cnt, err = p.GetCount(ctx, b.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(2), cnt)
	cnt, err = p.GetCount(ctx, c.Cid(), false)
	require.NoError(t, err)
	require.Equal(t, uint16(0), cnt)
}

func TestStreamKeys(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)

	// A{B,C{D,E},F}
	a := rndNode(t)
	b := rndNode(t)
	c := rndNode(t)
	d := rndNode(t)
	e := rndNode(t)
	f := rndNode(t)
	require.NoError(t, c.AddNodeLink("c0", d))
	require.NoError(t, c.AddNodeLink("c1", e))
	require.NoError(t, a.AddNodeLink("c0", b))
	require.NoError(t, a.AddNodeLink("c0", c))
	require.NoError(t, a.AddNodeLink("c0", f))

	require.NoError(t, dserv.Add(ctx, a))
	require.NoError(t, dserv.Add(ctx, b))
	require.NoError(t, dserv.Add(ctx, c))
	require.NoError(t, dserv.Add(ctx, d))
	require.NoError(t, dserv.Add(ctx, e))
	require.NoError(t, dserv.Add(ctx, f))

	require.NoError(t, p.Pin(ctx, a, true))
	require.NoError(t, p.Pin(ctx, a, true))
	require.NoError(t, p.Pin(ctx, b, true))

	require.NoError(t, p.Pin(ctx, a, false))
	require.NoError(t, p.Pin(ctx, c, false))
	require.NoError(t, p.Pin(ctx, d, false))
	require.NoError(t, p.Pin(ctx, d, false))
	require.NoError(t, p.Pin(ctx, e, false))

	cids := readCh(t, p.RecursiveKeys(ctx))
	require.Equal(t, 2, len(cids))
	require.True(t, cids[a.Cid()])
	require.True(t, cids[b.Cid()])

	cidCnts := readCountCh(t, p.RecursiveKeysWithCount(ctx))
	require.Equal(t, 2, len(cidCnts))
	require.Equal(t, 2, cidCnts[a.Cid()])
	require.Equal(t, 1, cidCnts[b.Cid()])

	cids = readCh(t, p.DirectKeys(ctx))
	require.Equal(t, 4, len(cids))
	require.True(t, cids[a.Cid()])
	require.True(t, cids[c.Cid()])
	require.True(t, cids[d.Cid()])
	require.True(t, cids[e.Cid()])

	cidCnts = readCountCh(t, p.DirectKeysWithCount(ctx))
	require.Equal(t, 4, len(cidCnts))
	require.Equal(t, 1, cidCnts[a.Cid()])
	require.Equal(t, 1, cidCnts[c.Cid()])
	require.Equal(t, 2, cidCnts[d.Cid()])
	require.Equal(t, 1, cidCnts[e.Cid()])
}

func TestCidIndex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore, dserv := makeStore(t)
	pinner, err := New(ctx, dstore, dserv)
	require.NoError(t, err)
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

func TestSizeStats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)

	a := rndNode(t)
	b := rndNode(t)
	c := rndNode(t)
	require.NoError(t, a.AddNodeLink("child_b", b))
	require.NoError(t, a.AddNodeLink("child_c", c))
	require.NoError(t, dserv.Add(ctx, a))
	require.NoError(t, dserv.Add(ctx, b))
	require.NoError(t, dserv.Add(ctx, c))

	sz := atomic.NewUint64(0)
	ctx = WithDagSize(ctx, sz)
	// Pin A{}
	require.NoError(t, p.Pin(ctx, a, true))
	cnt, err := p.GetCount(ctx, a.Cid(), true)
	require.NoError(t, err)
	require.Equal(t, uint16(1), cnt)
	sza := uint64(len(a.RawData()))
	szb := uint64(len(b.RawData()))
	szc := uint64(len(c.RawData()))
	require.Equal(t, sza+szb+szc, sz.Load())
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
	p pin.Pinner,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := range nodes {
		require.NoError(t, p.Pin(ctx, nodes[i], true /* don't care */))
	}
	require.NoError(t, p.Flush(ctx))
}

func unpinNodes(
	t require.TestingTB,
	nodes []ipld.Node,
	p pin.Pinner,
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
	pinner pin.Pinner,
	dserv ipld.DAGService,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := makeNodes(b, count, dserv)
	pinNodes(b, nodes[:count-1], pinner)
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
	pinner pin.Pinner,
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
	pinner pin.Pinner,
	dserv ipld.DAGService,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := makeNodes(b, count, dserv)
	pinNodes(b, nodes, pinner)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range nodes {
			// Unpin nodes one at a time.
			require.NoError(b, pinner.Unpin(ctx, nodes[j].Cid(), true))
			require.NoError(b, pinner.Flush(ctx))
		}
		// Pin all nodes so that they can be unpinned next iter.
		b.StopTimer()
		pinNodes(b, nodes, pinner)
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
	pinner pin.Pinner,
	dserv ipld.DAGService,
) {
	nodes := makeNodes(b, count, dserv)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pinNodes(b, nodes, pinner)

		b.StopTimer()
		unpinNodes(b, nodes, pinner)
		b.StartTimer()
	}
}

func readCh(t *testing.T, ch <-chan pin.StreamedCid) map[cid.Cid]bool {
	m := map[cid.Cid]bool{}
	for re := range ch {
		require.NoError(t, re.Err)
		m[re.C] = true
	}
	return m
}

func readCountCh(
	t *testing.T,
	ch <-chan *StreamedCidWithCount,
) map[cid.Cid]int {
	m := map[cid.Cid]int{}
	for re := range ch {
		require.NoError(t, re.Cid.Err)
		m[re.Cid.C] = int(re.Count)
	}
	return m
}
