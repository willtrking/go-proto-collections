package runtime

import (
	"errors"
	"fmt"
	"sync"

	"golang.org/x/net/context"

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
	//Force the loaded data to be returned as a list of interface{}
	//Returns nil if empty or not loaded
	InterfaceSlice() []interface{}
	//Register this loader in the provided registry for it's relevant collections
	//Register(CollectionRegistry)
}

type CollectionWriter interface {
	//Should use sync.Once internally to prevent double reads
	Read([]*ReadWriteContainer)
	//Validate the basic data, ideally with no IO interaction
	Validate(context.Context) (context.Context, []WriterError)
	//Validation with IO happens within this function
	CheckPrecondition(context.Context) (context.Context, []WriterError)
	//Write our data somewhere, can use IO
	Write(context.Context) ([]pcolh.CollectionWriterResponse, []WriterError)
}

func NewRegistry() *CollectionRegistry {

	return &CollectionRegistry{
		Loaders: make(map[RegistryKey]func(string) (CollectionLoader, error)),
		Writers: make(map[RegistryKey]func() (CollectionWriter, error)),
	}
}

//Allows us to register a loader to the relevant CollectionDetail struct
type CollectionRegistry struct {
	mu      sync.Mutex //Guard
	Loaders map[RegistryKey]func(string) (CollectionLoader, error)
	Writers map[RegistryKey]func() (CollectionWriter, error)
}

type RegistryKey struct {
	d pgcol.CollectionDetails
	p string
}

//Registers a function to get a new loader with a data key
func (r *CollectionRegistry) RegisterLoader(m pcolh.CollectionElem, f func(string) (CollectionLoader, error)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	k := r.Key(m)

	if _, ok := r.Loaders[k]; ok {
		grpclog.Println("Ignored duplicate collection loader registration of ", k)
		return
	}

	r.Loaders[k] = f

}

//Registers a function to get a new writer with a data key
func (r *CollectionRegistry) RegisterWriter(m pcolh.CollectionElem, f func() (CollectionWriter, error)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	k := r.Key(m)

	if _, ok := r.Writers[k]; ok {
		grpclog.Println("Ignored duplicate collection writer registration of ", k)
		return
	}

	r.Writers[k] = f

}

//Returns a function that will get a zero'd loader with the specified data key
//Each loader is stateful, so this is important
func (r *CollectionRegistry) Loader(m pcolh.CollectionElem) (func(string) (CollectionLoader, error), error) {

	k := r.Key(m)

	return r.LoaderForKey(k)

}

func (r *CollectionRegistry) LoaderForKey(k RegistryKey) (func(string) (CollectionLoader, error), error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.Loaders[k]; !ok {
		return nil, errors.New(fmt.Sprintf("No loader for %+v", k))
	}

	return r.Loaders[k], nil

}

//Returns a function that will get a zero'd writer with the specified data key
//Each writer is stateful, so this is important
func (r *CollectionRegistry) Writer(m pcolh.CollectionElem) (func() (CollectionWriter, error), error) {

	k := r.Key(m)

	return r.WriterForKey(k)

}

func (r *CollectionRegistry) WriterForKey(k RegistryKey) (func() (CollectionWriter, error), error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.Writers[k]; !ok {
		return nil, errors.New(fmt.Sprintf("No writer for %+v", k))
	}

	return r.Writers[k], nil

}

func (r *CollectionRegistry) Key(m pcolh.CollectionElem) RegistryKey {

	d := *m.DefaultDetails()

	return RegistryKey{
		d: d,
		p: proto.MessageName(m.DataProto()),
	}
}
