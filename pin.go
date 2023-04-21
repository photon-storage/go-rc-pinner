package rcpinner

import (
	"context"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	ipfspinner "github.com/ipfs/go-ipfs-pinner"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-merkledag"
)

const (
	basePath   = "/pins"
	rIndexPath = "/pins/idx_r"
	dIndexPath = "/pins/idx_d"
)

var (
	log logging.StandardLogger = logging.Logger("pin")

	linkDirect, linkRecursive string
)

func init() {
	directStr, ok := ipfspinner.ModeToString(ipfspinner.Direct)
	if !ok {
		panic("could not find Direct pin enum")
	}
	linkDirect = directStr

	recursiveStr, ok := ipfspinner.ModeToString(ipfspinner.Recursive)
	if !ok {
		panic("could not find Recursive pin enum")
	}
	linkRecursive = recursiveStr
}

var _ ipfspinner.Pinner = (*pinner)(nil)

type syncDAGService interface {
	ipld.DAGService
	Sync() error
}

type noSyncDAGService struct {
	ipld.DAGService
}

func (d *noSyncDAGService) Sync() error {
	return nil
}

// pinner implements the Pinner interface
type pinner struct {
	dstore   ds.Datastore
	dserv    syncDAGService
	cidDIdx  *index
	cidRIdx  *index
	autoSync bool
	clean    int64
	dirty    int64
	mu       sync.RWMutex
}

// New creates a new pinner and loads its keysets from the given datastore. If
// there is no data present in the datastore, then an empty pinner is returned.
//
// By default, changes are automatically flushed to the datastore.  This can be
// disabled by calling SetAutosync(false), which will require that Flush be
// called explicitly.
func New(
	ctx context.Context,
	dstore ds.Datastore,
	dserv ipld.DAGService,
) *pinner {
	syncDserv, ok := dserv.(syncDAGService)
	if !ok {
		syncDserv = &noSyncDAGService{dserv}
	}
	return &pinner{
		autoSync: true,
		cidDIdx:  newIndex(dstore, ds.NewKey(dIndexPath)),
		cidRIdx:  newIndex(dstore, ds.NewKey(rIndexPath)),
		dserv:    syncDserv,
		dstore:   dstore,
	}
}

// SetAutosync allows auto-syncing to be enabled or disabled during runtime.
// This may be used to turn off autosync before doing many repeated pinning
// operations, and then turn it on after.  Returns the previous value.
func (p *pinner) SetAutosync(auto bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.autoSync, auto = auto, p.autoSync
	return auto
}

// Pin the given node, optionally recursive
func (p *pinner) Pin(
	ctx context.Context,
	nd ipld.Node,
	recursive bool,
) error {
	if err := p.dserv.Add(ctx, nd); err != nil {
		return err
	}

	if recursive {
		return p.doPinRecursive(ctx, nd.Cid(), true)
	} else {
		return p.doPinDirect(ctx, nd.Cid())
	}
}

func (p *pinner) doPinRecursive(
	ctx context.Context,
	c cid.Cid,
	fetch bool,
) error {
	newPin, err := func() (bool, error) {
		p.mu.Lock()
		defer p.mu.Unlock()

		// Convert direct index to resursive index if needed.
		dcnt, err := p.cidDIdx.get(ctx, c)
		if err != nil {
			return false, err
		}

		// NOTE(kmax): the dec and inc is not atomic and can cause leak.
		if dcnt > 0 {
			if _, err := p.cidDIdx.dec(ctx, c, dcnt); err != nil {
				return false, err
			}
		}

		rcnt, err := p.cidRIdx.inc(ctx, c, dcnt+1)
		if err != nil {
			return false, err
		}

		if err := p.flushPins(ctx, false); err != nil {
			return false, err
		}

		return rcnt == dcnt+1, nil
	}()

	if err != nil {
		return err
	}

	if !newPin || !fetch {
		return nil
	}

	// Fetch graph starting at node identified by cid
	if err := merkledag.FetchGraph(ctx, c, p.dserv); err != nil {
		return err
	}

	// If autosyncing, sync dag service before making any change to pins
	if err := p.flushDagService(ctx, false); err != nil {
		return err
	}

	return nil
}

func (p *pinner) doPinDirect(ctx context.Context, c cid.Cid) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Convert direct index to resursive index if needed.
	rcnt, err := p.cidRIdx.get(ctx, c)
	if err != nil {
		return err
	}
	if rcnt > 0 {
		return fmt.Errorf("%s already pinned recursively", c.String())
	}

	if _, err := p.cidDIdx.inc(ctx, c, 1); err != nil {
		return err
	}

	if err := p.flushPins(ctx, false); err != nil {
		return err
	}

	return nil
}

// Unpin a given key
func (p *pinner) Unpin(ctx context.Context, c cid.Cid, recursive bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	rcnt, err := p.cidRIdx.get(ctx, c)
	if err != nil {
		return err
	}
	if rcnt > 0 {
		if !recursive {
			return fmt.Errorf("%s is pinned recursively", c.String())
		}
		if _, err := p.cidRIdx.dec(ctx, c, 1); err != nil {
			return err
		}
		return p.flushPins(ctx, false)
	}

	dcnt, err := p.cidDIdx.get(ctx, c)
	if err != nil {
		return err
	}
	if dcnt > 0 {
		if _, err := p.cidDIdx.dec(ctx, c, 1); err != nil {
			return err
		}
		return p.flushPins(ctx, false)
	}

	return ipfspinner.ErrNotPinned
}

// IsPinned returns whether or not the given key is pinned
// and an explanation of why its pinned
func (p *pinner) IsPinned(
	ctx context.Context,
	c cid.Cid,
) (string, bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isPinnedWithType(ctx, c, ipfspinner.Any)
}

// IsPinnedWithType returns whether or not the given cid is pinned with the
// given pin type, as well as returning the type of pin its pinned with.
func (p *pinner) IsPinnedWithType(
	ctx context.Context,
	c cid.Cid,
	mode ipfspinner.Mode,
) (string, bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.isPinnedWithType(ctx, c, mode)
}

func (p *pinner) isPinnedWithType(
	ctx context.Context,
	c cid.Cid,
	mode ipfspinner.Mode,
) (string, bool, error) {
	switch mode {
	case ipfspinner.Recursive:
		rcnt, err := p.cidRIdx.get(ctx, c)
		if err != nil {
			return "", false, err
		} else if rcnt > 0 {
			return linkRecursive, true, nil
		}
		return "", false, nil

	case ipfspinner.Direct:
		dcnt, err := p.cidDIdx.get(ctx, c)
		if err != nil {
			return "", false, err
		} else if dcnt > 0 {
			return linkDirect, true, nil
		}
		return "", false, nil

	case ipfspinner.Internal:
		return "", false, nil

	case ipfspinner.Indirect:

	case ipfspinner.Any:
		rcnt, err := p.cidRIdx.get(ctx, c)
		if err != nil {
			return "", false, err
		} else if rcnt > 0 {
			return linkRecursive, true, nil
		}
		dcnt, err := p.cidDIdx.get(ctx, c)
		if err != nil {
			return "", false, err
		} else if dcnt > 0 {
			return linkDirect, true, nil
		}
		// Continue to check indirect.

	default:
		return "", false,
			fmt.Errorf(
				"invalid Pin Mode '%d', must be one of {%d, %d, %d, %d, %d}",
				mode,
				ipfspinner.Direct,
				ipfspinner.Indirect,
				ipfspinner.Recursive,
				ipfspinner.Internal,
				ipfspinner.Any,
			)
	}

	// Default is Indirect
	visitedSet := cid.NewSet()

	// No index for given CID, so search children of all recursive pinned CIDs
	var has bool
	var rc cid.Cid
	if err := p.cidRIdx.forEach(
		ctx,
		func(rc cid.Cid, _ uint16) (bool, error) {
			var err error
			if has, err = hasChild(
				ctx,
				p.dserv,
				rc,
				c,
				visitedSet.Visit,
			); err != nil {
				return false, err
			}

			return !has, nil
		},
	); err != nil {
		return "", false, err
	}

	if has {
		return rc.String(), true, nil
	}

	return "", false, nil
}

// CheckIfPinned checks if a set of keys are pinned, more efficient than
// calling IsPinned for each key, returns the pinned status of cid(s)
//
// TODO: If a CID is pinned by multiple pins, should they all be reported?
func (p *pinner) CheckIfPinned(
	ctx context.Context,
	cids ...cid.Cid,
) ([]ipfspinner.Pinned, error) {
	pinned := make([]ipfspinner.Pinned, 0, len(cids))
	toCheck := cid.NewSet()

	p.mu.RLock()
	defer p.mu.RUnlock()

	// First check for non-Indirect pins directly
	for _, c := range cids {
		rcnt, err := p.cidRIdx.get(ctx, c)
		if err != nil {
			return nil, err
		} else if rcnt > 0 {
			pinned = append(pinned, ipfspinner.Pinned{
				Key:  c,
				Mode: ipfspinner.Recursive,
			})
		}
		dcnt, err := p.cidDIdx.get(ctx, c)
		if err != nil {
			return nil, err
		} else if dcnt > 0 {
			pinned = append(pinned, ipfspinner.Pinned{
				Key:  c,
				Mode: ipfspinner.Direct,
			})
		}

		if rcnt == 0 && dcnt == 0 {
			toCheck.Add(c)
		}
	}

	visited := cid.NewSet()
	if err := p.cidRIdx.forEach(
		ctx,
		func(rc cid.Cid, _ uint16) (bool, error) {
			if err := merkledag.Walk(
				ctx,
				merkledag.GetLinksWithDAG(p.dserv),
				rc,
				func(c cid.Cid) bool {
					if toCheck.Len() == 0 || !visited.Visit(c) {
						return false
					}

					if toCheck.Has(c) {
						pinned = append(pinned, ipfspinner.Pinned{
							Key:  c,
							Mode: ipfspinner.Indirect,
							Via:  rc,
						})
						toCheck.Remove(c)
					}

					return true
				},
				merkledag.Concurrent(),
			); err != nil {
				return false, err
			}

			return toCheck.Len() > 0, nil
		},
	); err != nil {
		return nil, err
	}

	// Anything left in toCheck is not pinned
	for _, k := range toCheck.Keys() {
		pinned = append(pinned, ipfspinner.Pinned{
			Key:  k,
			Mode: ipfspinner.NotPinned,
		})
	}

	return pinned, nil
}

// DirectKeys returns a slice containing the directly pinned keys
func (p *pinner) DirectKeys(ctx context.Context) ([]cid.Cid, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return getIndexKeys(ctx, p.cidDIdx)
}

// RecursiveKeys returns a slice containing the recursively pinned keys
func (p *pinner) RecursiveKeys(ctx context.Context) ([]cid.Cid, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return getIndexKeys(ctx, p.cidRIdx)
}

func getIndexKeys(
	ctx context.Context,
	idx *index,
) ([]cid.Cid, error) {
	cidSet := cid.NewSet()
	if err := idx.forEach(
		ctx,
		func(c cid.Cid, cnt uint16) (bool, error) {
			if cnt > 0 {
				cidSet.Add(c)
			}
			return true, nil
		},
	); err != nil {
		return nil, err
	}

	return cidSet.Keys(), nil
}

// InternalPins returns all cids kept pinned for the internal state of the
// pinner
func (p *pinner) InternalPins(ctx context.Context) ([]cid.Cid, error) {
	return nil, nil
}

func (p *pinner) Update(
	ctx context.Context,
	from cid.Cid,
	to cid.Cid,
	unpin bool,
) error {
	return ErrNotSupported
}

func (p *pinner) flushDagService(ctx context.Context, force bool) error {
	if !p.autoSync && !force {
		return nil
	}

	if err := p.dserv.Sync(); err != nil {
		return fmt.Errorf("cannot sync pinned data: %v", err)
	}

	return nil
}

func (p *pinner) flushPins(ctx context.Context, force bool) error {
	if !p.autoSync && !force {
		return nil
	}

	if err := p.dstore.Sync(ctx, ds.NewKey(basePath)); err != nil {
		return fmt.Errorf("cannot sync pin state: %v", err)
	}

	return nil
}

// Flush encodes and writes pinner keysets to the datastore
func (p *pinner) Flush(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.flushDagService(ctx, true)
	if err != nil {
		return err
	}

	return p.flushPins(ctx, true)
}

// PinWithMode allows the user to have fine grained control over pin
// counts
func (p *pinner) PinWithMode(
	ctx context.Context,
	c cid.Cid,
	mode ipfspinner.Mode,
) error {
	switch mode {
	case ipfspinner.Recursive:
		return p.doPinRecursive(ctx, c, false)
	case ipfspinner.Direct:
		return p.doPinDirect(ctx, c)
	default:
		return fmt.Errorf("unrecognized pin mode")
	}
}

// hasChild recursively looks for a Cid among the children of a root Cid.
// The visit function can be used to shortcut already-visited branches.
func hasChild(
	ctx context.Context,
	ng ipld.NodeGetter,
	root cid.Cid,
	child cid.Cid,
	visit func(cid.Cid) bool,
) (bool, error) {
	links, err := ipld.GetLinks(ctx, ng, root)
	if err != nil {
		return false, err
	}

	for _, lnk := range links {
		c := lnk.Cid
		if lnk.Cid.Equals(child) {
			return true, nil
		}

		if visit(c) {
			has, err := hasChild(ctx, ng, c, child, visit)
			if err != nil {
				return false, err
			}

			if has {
				return has, nil
			}
		}
	}

	return false, nil
}
