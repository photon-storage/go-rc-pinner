package rcpinner

import (
	"context"
	"os"
	"testing"

	bs "github.com/ipfs/go-blockservice"
	ds "github.com/ipfs/go-datastore"
	bds "github.com/ipfs/go-ds-badger"
	lds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipld "github.com/ipfs/go-ipld-format"
	mdag "github.com/ipfs/go-merkledag"

	"github.com/photon-storage/go-common/testing/require"
)

func makeStoreLevelDB(
	b *testing.B,
	dir string,
) (ds.Datastore, ipld.DAGService) {
	ldstore, err := lds.NewDatastore(dir, nil)
	require.NoError(b, err)

	dstore := ldstore
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	return dstore, dserv
}

func makeStoreBadger(
	b *testing.B,
	dir string,
) (ds.Datastore, ipld.DAGService) {
	bdstore, err := bds.NewDatastore(dir, nil)
	require.NoError(b, err)

	dstore := &batchWrap{bdstore}
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	return dstore, dserv
}

func benchAutoSync(
	b *testing.B,
	N int,
	auto bool,
	dstore ds.Datastore,
	dserv ipld.DAGService,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pinner, err := New(ctx, dstore, dserv)
	require.NoError(b, err)

	nodes := makeNodes(b, N, dserv)

	pinner.SetAutosync(auto)
	pinNodes(b, nodes, pinner, true)
}

func BenchmarkSyncOnceBadger(b *testing.B) {
	const dsDir = "b-once"
	dstoreB1, dservB1 := makeStoreBadger(b, dsDir)
	defer os.RemoveAll(dsDir)
	benchAutoSync(b, b.N, false, dstoreB1, dservB1)
	dstoreB1.Close()
}

func BenchmarkSyncEveryBadger(b *testing.B) {
	const dsDir = "b-every"
	dstoreB2, dservB2 := makeStoreBadger(b, dsDir)
	defer os.RemoveAll(dsDir)
	benchAutoSync(b, b.N, true, dstoreB2, dservB2)
	dstoreB2.Close()
}

func BenchmarkSyncOnceLevelDB(b *testing.B) {
	const dsDir = "l-once"
	dstoreL1, dservL1 := makeStoreLevelDB(b, dsDir)
	defer os.RemoveAll(dsDir)
	benchAutoSync(b, b.N, false, dstoreL1, dservL1)
	dstoreL1.Close()
}

func BenchmarkSyncEveryLevelDB(b *testing.B) {
	const dsDir = "l-every"
	dstoreL2, dservL2 := makeStoreLevelDB(b, dsDir)
	defer os.RemoveAll(dsDir)
	benchAutoSync(b, b.N, true, dstoreL2, dservL2)
	dstoreL2.Close()
}
