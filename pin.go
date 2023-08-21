package rcpinner

import (
	"context"
	"fmt"
	"sync"

	"github.com/ipfs/boxo/ipld/merkledag"
	pin "github.com/ipfs/boxo/pinning/pinner"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	ipld "github.com/ipfs/go-ipld-format"
)

const (
	basePath   = "/pins"
	rIndexPath = "/pins/idx_r"
	dIndexPath = "/pins/idx_d"
)

var (
	linkRecursive string
	linkDirect    string
)

func init() {
	recursiveStr, ok := pin.ModeToString(pin.Recursive)
	if !ok {
		panic("could not find recursive pin enum")
	}
	linkRecursive = recursiveStr
	directStr, ok := pin.ModeToString(pin.Direct)
	if !ok {
		panic("could not find direct pin enum")
	}
	linkDirect = directStr
}

var _ pin.Pinner = (*RcPinner)(nil)

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

// RcPinner implements the Pinner interface
type RcPinner struct {
	dstore   ds.Datastore
	dserv    syncDAGService
	cidRIdx  *index
	cidDIdx  *index
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
) (*RcPinner, error) {
	syncDserv, ok := dserv.(syncDAGService)
	if !ok {
		syncDserv = &noSyncDAGService{dserv}
	}
	cidRIdx, err := newIndex(ctx, dstore, ds.NewKey(rIndexPath))
	if err != nil {
		return nil, err
	}
	cidDIdx, err := newIndex(ctx, dstore, ds.NewKey(dIndexPath))
	if err != nil {
		return nil, err
	}
	return &RcPinner{
		autoSync: true,
		cidRIdx:  cidRIdx,
		cidDIdx:  cidDIdx,
		dserv:    syncDserv,
		dstore:   dstore,
	}, nil
}

// SetAutosync allows auto-syncing to be enabled or disabled during runtime.
// This may be used to turn off autosync before doing many repeated pinning
// operations, and then turn it on after.  Returns the previous value.
func (p *RcPinner) SetAutosync(auto bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.autoSync, auto = auto, p.autoSync
	return auto
}

// Pin the given node, optionally recursive
func (p *RcPinner) Pin(
	ctx context.Context,
	nd ipld.Node,
	recursive bool,
) error {
	if err := p.dserv.Add(ctx, nd); err != nil {
		return err
	}

	if recursive {
		return p.doPinRecursive(ctx, nd.Cid(), true)
	}

	return p.doPinDirect(ctx, nd.Cid())
}

func (p *RcPinner) doPinDirect(
	ctx context.Context,
	c cid.Cid,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, err := p.cidDIdx.inc(ctx, c, 1); err != nil {
		return err
	}

	if err := p.flushPins(ctx, false); err != nil {
		return err
	}

	return nil
}

func (p *RcPinner) doPinRecursive(
	ctx context.Context,
	c cid.Cid,
	fetch bool,
) error {
	// NOTE(kmax): fetch DAG data first before bump the index count.
	// This is to ensure that when the count is bumped, the data is guaranteed
	// to exist locally (unless they are GC'ed whiling being fetched).
	// Failure to bump the count after data fetch is fine. The data can be
	// purged by GC without harm.
	if fetch {
		// Fetch graph starting at node identified by cid
		if err := FetchGraphWithDepthLimit(ctx, c, -1, p.dserv); err != nil {
			return err
		}

		// If autosyncing, sync dag service before making any change to pins
		if err := p.flushDagService(ctx, false); err != nil {
			return err
		}
	}

	if err := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()

		if _, err := p.cidRIdx.inc(ctx, c, 1); err != nil {
			return err
		}

		if err := p.flushPins(ctx, false); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		return err
	}

	return nil
}

// Unpin a given key
func (p *RcPinner) Unpin(ctx context.Context, c cid.Cid, recursive bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if recursive {
		rcnt, err := p.cidRIdx.get(ctx, c)
		if err != nil {
			return err
		}

		if rcnt == 0 {
			return pin.ErrNotPinned
		}

		if _, err := p.cidRIdx.dec(ctx, c, 1); err != nil {
			return err
		}
	} else {
		rcnt, err := p.cidDIdx.get(ctx, c)
		if err != nil {
			return err
		}

		if rcnt == 0 {
			return pin.ErrNotPinned
		}

		if _, err := p.cidDIdx.dec(ctx, c, 1); err != nil {
			return err
		}
	}

	return p.flushPins(ctx, false)
}

// GetCount returns the reference count pinned in the index for the
// given CID. The API looks up only the exact CID given in the index.
// It does not check descendent recursively.
func (p *RcPinner) GetCount(
	ctx context.Context,
	c cid.Cid,
	recursively bool,
) (uint16, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if recursively {
		rcnt, err := p.cidRIdx.get(ctx, c)
		if err != nil {
			return 0, err
		}
		return rcnt, nil
	}

	rcnt, err := p.cidDIdx.get(ctx, c)
	if err != nil {
		return 0, err
	}

	return rcnt, nil
}

// IncCount increases reference count for the given CID. This is a shortcut for
// adding a reference count without going through the heavy pinning process.
func (p *RcPinner) IncCount(
	ctx context.Context,
	c cid.Cid,
	recursive bool,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if recursive {
		if _, err := p.cidRIdx.inc(ctx, c, 1); err != nil {
			return err
		}
	} else {
		if _, err := p.cidDIdx.inc(ctx, c, 1); err != nil {
			return err
		}
	}

	if err := p.flushPins(ctx, false); err != nil {
		return err
	}

	return nil
}

// DecCount decreases reference count for the given CID. Same as Unpin.
func (p *RcPinner) DecCount(
	ctx context.Context,
	c cid.Cid,
	recursive bool,
) error {
	return p.Unpin(ctx, c, recursive)
}

// IsPinned returns whether or not the given key is pinned
// and an explanation of why its pinned
func (p *RcPinner) IsPinned(
	ctx context.Context,
	c cid.Cid,
) (string, bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isPinnedWithType(ctx, c, pin.Any)
}

// IsPinnedWithType returns whether or not the given cid is pinned with the
// given pin type, as well as returning the type of pin its pinned with.
func (p *RcPinner) IsPinnedWithType(
	ctx context.Context,
	c cid.Cid,
	mode pin.Mode,
) (string, bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.isPinnedWithType(ctx, c, mode)
}

func (p *RcPinner) isPinnedWithType(
	ctx context.Context,
	c cid.Cid,
	mode pin.Mode,
) (string, bool, error) {
	switch mode {
	case pin.Recursive:
		rcnt, err := p.cidRIdx.get(ctx, c)
		if err != nil {
			return "", false, err
		} else if rcnt > 0 {
			return linkRecursive, true, nil
		}
		return "", false, nil

	case pin.Direct:
		rcnt, err := p.cidDIdx.get(ctx, c)
		if err != nil {
			return "", false, err
		} else if rcnt > 0 {
			return linkDirect, true, nil
		}
		return "", false, nil

	case pin.Internal:
		return "", false, nil

	case pin.Indirect:

	case pin.Any:
		rcnt, err := p.cidRIdx.get(ctx, c)
		if err != nil {
			return "", false, err
		} else if rcnt > 0 {
			return linkRecursive, true, nil
		}
		rcnt, err = p.cidDIdx.get(ctx, c)
		if err != nil {
			return "", false, err
		} else if rcnt > 0 {
			return linkDirect, true, nil
		}
		// Continue to check indirect.

	default:
		return "", false,
			fmt.Errorf(
				"invalid Pin Mode '%d', must be one of {%d, %d, %d, %d, %d}",
				mode,
				pin.Direct,
				pin.Indirect,
				pin.Recursive,
				pin.Internal,
				pin.Any,
			)
	}

	// Default is Indirect
	visitedSet := cid.NewSet()

	// No index for given CID, so search children of all recursive pinned CIDs
	var has bool
	var k cid.Cid
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
			if has {
				k = rc
			}
			return !has, nil
		},
	); err != nil {
		return "", false, err
	}

	if has {
		return k.String(), true, nil
	}

	return "", false, nil
}

// CheckIfPinned checks if a set of keys are pinned, more efficient than
// calling IsPinned for each key, returns the pinned status of cid(s)
//
// TODO: If a CID is pinned by multiple pins, should they all be reported?
func (p *RcPinner) CheckIfPinned(
	ctx context.Context,
	cids ...cid.Cid,
) ([]pin.Pinned, error) {
	pinned := make([]pin.Pinned, 0, len(cids))
	toCheck := cid.NewSet()

	p.mu.RLock()
	defer p.mu.RUnlock()

	// First check for non-Indirect pins directly
	for _, c := range cids {
		rrcnt, err := p.cidRIdx.get(ctx, c)
		if err != nil {
			return nil, err
		}
		drcnt, err := p.cidDIdx.get(ctx, c)
		if err != nil {
			return nil, err
		}

		if rrcnt > 0 && drcnt > 0 {
			pinned = append(pinned, pin.Pinned{
				Key:  c,
				Mode: pin.Any,
			})
		} else if rrcnt > 0 {
			pinned = append(pinned, pin.Pinned{
				Key:  c,
				Mode: pin.Recursive,
			})
		} else if drcnt > 0 {
			pinned = append(pinned, pin.Pinned{
				Key:  c,
				Mode: pin.Direct,
			})
		} else {
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
						pinned = append(pinned, pin.Pinned{
							Key:  c,
							Mode: pin.Indirect,
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
		pinned = append(pinned, pin.Pinned{
			Key:  k,
			Mode: pin.NotPinned,
		})
	}

	return pinned, nil
}

// DirectKeys returns a slice containing the directly pinned keys
func (p *RcPinner) DirectKeys(ctx context.Context) <-chan pin.StreamedCid {
	out := make(chan pin.StreamedCid)
	re := make(chan *StreamedCidWithCount)
	go func() {
		p.mu.RLock()
		defer p.mu.RUnlock()
		defer close(re)

		keysWithCount(ctx, p.cidDIdx, re)
	}()

	go func() {
		defer close(out)
		for v := range re {
			out <- v.Cid
		}
	}()

	return out
}

// DirectKeysWithCount streams out directly pinned keys with reference count.
func (p *RcPinner) DirectKeysWithCount(
	ctx context.Context,
) <-chan *StreamedCidWithCount {
	out := make(chan *StreamedCidWithCount)
	go func() {
		p.mu.RLock()
		defer p.mu.RUnlock()
		defer close(out)
		keysWithCount(ctx, p.cidDIdx, out)
	}()

	return out
}

// RecursiveKeys streams out recursively pinned keys
func (p *RcPinner) RecursiveKeys(ctx context.Context) <-chan pin.StreamedCid {
	out := make(chan pin.StreamedCid)
	re := make(chan *StreamedCidWithCount)
	go func() {
		p.mu.RLock()
		defer p.mu.RUnlock()
		defer close(re)
		keysWithCount(ctx, p.cidRIdx, re)
	}()

	go func() {
		defer close(out)
		for v := range re {
			out <- v.Cid
		}
	}()

	return out
}

// RecursiveKeysWithCount streams out recursively pinned keys with
// reference count.
func (p *RcPinner) RecursiveKeysWithCount(
	ctx context.Context,
) <-chan *StreamedCidWithCount {
	out := make(chan *StreamedCidWithCount)
	go func() {
		p.mu.RLock()
		defer p.mu.RUnlock()
		defer close(out)
		keysWithCount(ctx, p.cidRIdx, out)
	}()

	return out
}

type StreamedCidWithCount struct {
	Cid   pin.StreamedCid
	Count uint16
}

// keysWithCount streams out pinned keys with corresponding reference count.
func keysWithCount(
	ctx context.Context,
	idx *index,
	out chan *StreamedCidWithCount,
) {
	cidSet := cid.NewSet()
	if err := idx.forEach(
		ctx,
		func(c cid.Cid, cnt uint16) (bool, error) {
			if cidSet.Has(c) || cnt == 0 {
				return true, nil
			}

			select {
			case <-ctx.Done():
				return false, nil
			case out <- &StreamedCidWithCount{
				Cid:   pin.StreamedCid{C: c},
				Count: cnt,
			}:
			}

			cidSet.Add(c)
			return true, nil
		},
	); err != nil {
		out <- &StreamedCidWithCount{
			Cid: pin.StreamedCid{Err: err},
		}
	}
}

// InternalPins returns all cids kept pinned for the internal state of the
// pinner
func (p *RcPinner) InternalPins(ctx context.Context) <-chan pin.StreamedCid {
	out := make(chan pin.StreamedCid)
	close(out)
	return out
}

func (p *RcPinner) Update(
	ctx context.Context,
	from cid.Cid,
	to cid.Cid,
	unpin bool,
) error {
	return ErrUpdateUnsupported
}

func (p *RcPinner) flushDagService(ctx context.Context, force bool) error {
	if !p.autoSync && !force {
		return nil
	}

	if err := p.dserv.Sync(); err != nil {
		return fmt.Errorf("cannot sync pinned data: %v", err)
	}

	return nil
}

func (p *RcPinner) flushPins(ctx context.Context, force bool) error {
	if !p.autoSync && !force {
		return nil
	}

	if err := p.dstore.Sync(ctx, ds.NewKey(basePath)); err != nil {
		return fmt.Errorf("cannot sync pin state: %v", err)
	}

	return nil
}

// Flush encodes and writes pinner keysets to the datastore
func (p *RcPinner) Flush(ctx context.Context) error {
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
func (p *RcPinner) PinWithMode(
	ctx context.Context,
	c cid.Cid,
	mode pin.Mode,
) error {
	switch mode {
	case pin.Recursive:
		return p.doPinRecursive(ctx, c, true)

	case pin.Direct:
		return p.doPinDirect(ctx, c)

	default:
		return fmt.Errorf("unrecognized pin mode")
	}
}

// TotalPinnedCount returns total reference count pinned in the index.
func (p *RcPinner) TotalPinnedCount(recursively bool) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	if recursively {
		return p.cidRIdx.totalCount()
	}

	return p.cidDIdx.totalCount()
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
