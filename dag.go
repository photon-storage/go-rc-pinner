package rcpinner

import (
	"context"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
)

// NOTE(kmax): function forked from github.com/ipfs/boxo/ipld/merkledag package
// with refactoring to support with dag size stats.
// FetchGraphWithDepthLimit fetches all nodes that are children to the given
// node down to the given depth. maxDepth=0 means "only fetch root",
// maxDepth=1 means "fetch root and its direct children" and so on...
// maxDepth=-1 means unlimited.
func FetchGraphWithDepthLimit(
	ctx context.Context,
	root cid.Cid,
	depthLim int,
	serv format.DAGService,
) error {
	var ng format.NodeGetter = merkledag.NewSession(ctx, serv)

	set := make(map[cid.Cid]int)
	// Visit function returns true when:
	// * The element is not in the set and we're not over depthLim
	// * The element is in the set but recorded depth is deeper
	//   than currently seen (if we find it higher in the tree we'll need
	//   to explore deeper than before).
	// depthLim = -1 means we only return true if the element is not in the
	// set.
	visit := func(c cid.Cid, depth int) bool {
		oldDepth, ok := set[c]

		if (ok && depthLim < 0) || (depthLim >= 0 && depth > depthLim) {
			return false
		}

		if !ok || oldDepth > depth {
			set[c] = depth
			return true
		}
		return false
	}

	st := DagSize(ctx)
	pt := ProgressTracker(ctx)
	cc := Concurrency(ctx)
	if cc == 0 {
		cc = 32
	}
	return merkledag.WalkDepth(
		ctx,
		func(
			ctx context.Context,
			c cid.Cid,
		) ([]*format.Link, error) {
			nd, err := ng.Get(ctx, c)
			if err != nil {
				return nil, err
			}

			links := nd.Links()
			if st != nil {
				var sz uint64
				if len(links) == 0 {
					if sz, err = nd.Size(); err != nil {
						return nil, err
					}
				} else {
					sz = uint64(len(nd.RawData()))
				}
				st.Add(sz)
			}

			return links, nil
		},
		root,
		func(c cid.Cid, depth int) bool {
			if visit(c, depth) {
				if pt != nil {
					pt.Increment()
				}
				return true
			}
			return false
		},
		merkledag.Concurrency(cc),
	)
}
