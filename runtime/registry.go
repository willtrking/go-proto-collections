package runtime

import (
	"errors"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	pcolh "github.com/willtrking/go-proto-collections/helpers"
	pgcol "github.com/willtrking/go-proto-collections/protocollections"
	"google.golang.org/grpc/grpclog"
)

//Interface for structs which loads a collection into it's concrete type
type CollectionLoader interface {
	//Provide the key to use to load data with (e.g. id, product, etc.)
	//Useful so 1 loader can handle many collections (don't need to hardcode in columns)
	//Return an error if we don't know that key, otherwise nil
	SetDataKey(string) error
	//Should use sync.Once internally to prevent double loads
	Load(from []interface{})
	//We want to be able to determine if the loader is loading
	Loading() bool
	//We want to be able to determine if the loader is loaded
	Loaded() bool
	//We want to be able to wait for a loader to have called load
	Wait()
	//Return a new version of the itself, ready to be used
	New() CollectionLoader
	//Force the loaded data to be returned as a list of interface{}
	//Returns nil if empty or not loaded
	DataSlice() []interface{}
	//Register this loader in the provided registry for it's relevant collections
	//Register(CollectionRegistry)
}

func NewRegistry() *CollectionRegistry {

	return &CollectionRegistry{
		Loaders: make(map[RegistryKey]*CollectionLoader),
	}
}

//Allows us to register a loader to the relevant CollectionDetail struct
type CollectionRegistry struct {
	mu      sync.Mutex //Guard
	Loaders map[RegistryKey]*CollectionLoader
}

type RegistryKey struct {
	d pgcol.CollectionDetails
	p string
}

//Registers a loader
func (r *CollectionRegistry) RegisterLoader(m pcolh.CollectionElem, f CollectionLoader) {
	r.mu.Lock()
	defer r.mu.Unlock()

	d := *m.DefaultDetails()
	k := RegistryKey{
		d: d,
		p: proto.MessageName(m.DataProto()),
	}

	if _, ok := r.Loaders[k]; ok {
		grpclog.Println("Ignored duplicate collection registration of ", k)
		return
	}

	r.Loaders[k] = &f

}

//Returns a new loader of the zero value concrete type behind our CollectionLoader
//Each loader is stateful, so this is important
func (r *CollectionRegistry) Loader(m pcolh.CollectionElem) (CollectionLoader, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	d := *m.DefaultDetails()
	k := RegistryKey{
		d: d,
		p: proto.MessageName(m.DataProto()),
	}

	if _, ok := r.Loaders[k]; !ok {
		return nil, errors.New(fmt.Sprintf("No loader for %+v", k))
	}

	return (*r.Loaders[k]).New(), nil

}
