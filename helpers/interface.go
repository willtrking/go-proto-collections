package helpers

import proto "github.com/golang/protobuf/proto"

//Define interfaces that help us match generated messages

type CollectionElem interface {
	DefaultDetails() *CollectionDetails
	DataProto() proto.Message
	LoadData(data interface{})
	//Force data to come back as a slice
	DataSlice() []interface{}
	//SetupDetails(*pb.CollectionDetails)
}

type CollectionMessage interface {
	DefaultCollectionMap() map[string]CollectionElem
	LoadCollection(collection string, data interface{}) error
	CollectionDataSlice(collection string) []interface{}
}
