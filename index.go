package rcpinner

import (
	"context"
	"encoding/binary"
	"fmt"
	"path"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	"github.com/multiformats/go-multibase"
)

const (
	totalCountKey = "metadata:tc"
)

type index struct {
	dstore  ds.Datastore
	totalRc uint64
}

// newIndex creates a new datastore index. All indexes are stored under
// the specified index name prefix.
//
// To persist the actions of calling index functions, it is necessary to
// call dstore.Sync.
func newIndex(
	ctx context.Context,
	dstore ds.Datastore,
	prefix ds.Key,
) (*index, error) {
	idx := &index{
		dstore: namespace.Wrap(dstore, prefix),
	}

	cnt, err := idx.readTotalCount(ctx)
	if err == ds.ErrNotFound {
		// Upgrade existing index.
		if err := idx.forEach(
			ctx,
			func(_ cid.Cid, v uint16) (bool, error) {
				cnt += uint64(v)
				return true, nil
			},
		); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	idx.totalRc = cnt
	return idx, nil
}

func (x *index) inc(
	ctx context.Context,
	c cid.Cid,
	v uint16,
) (uint16, error) {
	if !c.Defined() {
		return 0, ErrEmptyKey
	}

	key := ds.NewKey(encodeKey(c))

	val, err := x.dstore.Get(ctx, key)
	if err != nil && err != ds.ErrNotFound {
		return 0, err
	}

	var cnt uint16
	if err == nil {
		cnt, err = decodeCount(val)
		if err != nil {
			return 0, err
		}
		val = val[:0]
	}

	if int64(cnt)+int64(v) > 65535 {
		return 0, ErrPinCountOverflow
	}

	cnt += v
	val = encodeCountWithAppend(val, cnt)

	if err := x.dstore.Put(ctx, key, val); err != nil {
		return 0, err
	}

	if err := x.updateTotalCount(ctx, int64(v)); err != nil {
		return 0, err
	}

	return cnt, nil
}

func (x *index) dec(
	ctx context.Context,
	c cid.Cid,
	v uint16,
) (uint16, error) {
	if !c.Defined() {
		return 0, ErrEmptyKey
	}

	key := ds.NewKey(encodeKey(c))

	val, err := x.dstore.Get(ctx, key)
	if err != nil {
		return 0, err
	}

	cnt, err := decodeCount(val)
	if err != nil {
		return 0, err
	}

	if int64(cnt)-int64(v) < 0 {
		return 0, ErrPinCountUnderflow
	}
	cnt -= v

	if cnt == 0 {
		if err := x.dstore.Delete(ctx, key); err != nil {
			return 0, err
		}
	} else {
		val = encodeCountWithAppend(val[:0], cnt)
		if err := x.dstore.Put(ctx, key, val); err != nil {
			return 0, err
		}
	}

	if err := x.updateTotalCount(ctx, -int64(v)); err != nil {
		return 0, err
	}

	return cnt, nil
}

func (x *index) get(ctx context.Context, c cid.Cid) (uint16, error) {
	if !c.Defined() {
		return 0, ErrEmptyKey
	}

	key := ds.NewKey(encodeKey(c))

	val, err := x.dstore.Get(ctx, key)
	if err != nil {
		if err == ds.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}

	return decodeCount(val)
}

func (x *index) forEach(
	ctx context.Context,
	fn func(cid.Cid, uint16) (bool, error),
) error {
	res, err := x.dstore.Query(
		ctx,
		query.Query{
			Prefix:   "",
			KeysOnly: false,
		},
	)
	if err != nil {
		return err
	}
	defer res.Close()

	for r := range res.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.Error != nil {
			return fmt.Errorf(
				"error advancing index query result: %v", r.Error)
		}

		k := path.Base(r.Entry.Key)
		if k == totalCountKey {
			continue
		}

		c, err := decodeKey(k)
		if err != nil {
			return fmt.Errorf("error decoding cid: %v", err)
		}

		cnt, err := decodeCount(r.Entry.Value)
		if err != nil {
			return fmt.Errorf("error decoding count: %v", err)
		}

		cont, err := fn(c, cnt)
		if err != nil {
			return err
		}

		if !cont {
			return nil
		}
	}

	return nil
}

func (x *index) totalCount() uint64 {
	return x.totalRc
}

func (x *index) readTotalCount(ctx context.Context) (uint64, error) {
	v, err := x.dstore.Get(ctx, ds.NewKey(totalCountKey))
	if err != nil {
		return 0, err
	}

	return decodeCount64(v)
}

func (x *index) updateTotalCount(ctx context.Context, v int64) error {
	if v > 0 {
		x.totalRc += uint64(v)
	} else {
		x.totalRc -= uint64(-v)
	}
	val := encodeCount64(make([]byte, 8), x.totalRc)
	return x.dstore.Put(ctx, ds.NewKey(totalCountKey), val)
}

func encodeCountWithAppend(b []byte, cnt uint16) []byte {
	return binary.LittleEndian.AppendUint16(b, cnt)
}

func decodeCount(v []byte) (uint16, error) {
	if len(v) != 2 {
		return 0, ErrInvalidValue
	}

	return binary.LittleEndian.Uint16(v), nil
}

func encodeCount64(b []byte, cnt uint64) []byte {
	binary.LittleEndian.PutUint64(b, cnt)
	return b
}

func decodeCount64(v []byte) (uint64, error) {
	if len(v) != 8 {
		return 0, ErrInvalidValue
	}

	return binary.LittleEndian.Uint64(v), nil
}

func encodeKey(c cid.Cid) string {
	encData, err := multibase.Encode(
		multibase.Base64url,
		[]byte(c.KeyString()),
	)
	if err != nil {
		// programming error; using unsupported encoding
		panic(err.Error())
	}
	return encData
}

func decodeKey(data string) (cid.Cid, error) {
	_, decData, err := multibase.Decode(data)
	if err != nil {
		return cid.Undef, err
	}
	return cid.Cast(decData)
}
