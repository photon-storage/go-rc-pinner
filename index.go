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

type index struct {
	dstore ds.Datastore
}

// newIndex creates a new datastore index. All indexes are stored under
// the specified index name prefix.
//
// To persist the actions of calling index functions, it is necessary to
// call dstore.Sync.
func newIndex(dstore ds.Datastore, prefix ds.Key) *index {
	return &index{
		dstore: namespace.Wrap(dstore, prefix),
	}
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
	val = encodeCount(val, cnt)

	if err := x.dstore.Put(ctx, key, val); err != nil {
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
		return 0, x.dstore.Delete(ctx, key)
	} else {
		val = encodeCount(val[:0], cnt)
		if err := x.dstore.Put(ctx, key, val); err != nil {
			return 0, err
		}
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

		c, err := decodeKey(path.Base(r.Entry.Key))
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

func encodeCount(b []byte, cnt uint16) []byte {
	return binary.LittleEndian.AppendUint16(b, cnt)
}

func decodeCount(v []byte) (uint16, error) {
	if len(v) != 2 {
		return 0, ErrInvalidValue
	}

	return binary.LittleEndian.Uint16(v), nil
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
