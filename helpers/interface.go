package helpers

import (
	proto "github.com/golang/protobuf/proto"
	pgcol "github.com/willtrking/go-proto-collections/protocollections"
)

//Define interfaces that help us match generated messages

type CollectionElem interface {
	DefaultDetails() *pgcol.CollectionDetails
	DataProto() proto.Message
	LoadData(data interface{})
	//Force data to come back as a slice
	DataSlice() []interface{}
}

type CollectionMessage interface {
	DefaultCollectionMap() map[string]CollectionElem
	LoadCollection(collection string, data interface{}) error
	CollectionDataSlice(collection string) []interface{}
}
