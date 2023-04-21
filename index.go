package rcpinner

import (
	"context"
	"fmt"
	"path"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	"github.com/multiformats/go-multibase"
)

// TODO: Consider adding caching
type index struct {
	dstore ds.Datastore
}

// newIndex creates a new datastore index. All indexes are stored under
// the specified index name prefix.
//
// To persist the actions of calling index functions, it is necessary to
// call dstore.Sync.
func newIndex(dstore ds.Datastore, name ds.Key) *index {
	return &index{
		dstore: namespace.Wrap(dstore, name),
	}
}

func (x *index) add(ctx context.Context, k, v string) error {
	if k == "" {
		return ErrEmptyKey
	}

	if v == "" {
		return ErrEmptyValue
	}

	return x.dstore.Put(
		ctx,
		ds.NewKey(encode(k)).ChildString(encode(v)),
		[]byte{},
	)
}

func (x *index) del(ctx context.Context, k, v string) error {
	if k == "" {
		return ErrEmptyKey
	}

	if v == "" {
		return ErrEmptyValue
	}

	return x.dstore.Delete(
		ctx,
		ds.NewKey(encode(k)).ChildString(encode(v)),
	)
}

func (x *index) delKey(ctx context.Context, k string) error {
	if k == "" {
		return ErrEmptyKey
	}

	return x.forEach(ctx, k, func(k, v string) (bool, error) {
		return true, x.dstore.Delete(
			ctx,
			ds.NewKey(encode(k)).ChildString(encode(v)),
		)
	})
}

func (x *index) forEach(
	ctx context.Context,
	prefix string,
	fn func(k, v string) (bool, error),
) error {
	if prefix != "" {
		prefix = encode(prefix)
	}

	res, err := x.dstore.Query(
		ctx,
		query.Query{
			Prefix:   prefix,
			KeysOnly: true,
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

		p := r.Entry.Key
		k, err := decode(path.Base(path.Dir(p)))
		if err != nil {
			return fmt.Errorf("error decoding index key: %v", err)
		}

		v, err := decode(path.Base(p))
		if err != nil {
			return fmt.Errorf("error decoding index value: %v", err)
		}

		cont, err := fn(k, v)
		if err != nil {
			return err
		}

		if !cont {
			return nil
		}
	}

	return nil
}

func (x *index) HasValue(ctx context.Context, k, v string) (bool, error) {
	if k == "" {
		return false, ErrEmptyKey
	}

	if v == "" {
		return false, ErrEmptyValue
	}

	return x.dstore.Has(
		ctx,
		ds.NewKey(encode(k)).ChildString(encode(v)),
	)
}

func (x *index) HasAny(ctx context.Context, k string) (bool, error) {
	var any bool
	return any, x.forEach(
		ctx,
		k,
		func(k, v string) (bool, error) {
			any = true
			return false, nil
		},
	)
}

func (x *index) Search(ctx context.Context, k string) ([]string, error) {
	if k == "" {
		return nil, ErrEmptyKey
	}

	var values []string
	if err := x.forEach(
		ctx,
		k,
		func(k, v string) (bool, error) {
			values = append(values, v)
			return true, nil
		},
	); err != nil {
		return nil, err
	}

	return values, nil
}

func encode(data string) string {
	encData, err := multibase.Encode(multibase.Base64url, []byte(data))
	if err != nil {
		// programming error; using unsupported encoding
		panic(err.Error())
	}
	return encData
}

func decode(data string) (string, error) {
	_, b, err := multibase.Decode(data)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
