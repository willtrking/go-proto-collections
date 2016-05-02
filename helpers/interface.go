package helpers

import (
	proto "github.com/golang/protobuf/proto"
	pgcol "github.com/willtrking/go-proto-collections/protocollections"
)

//Define interfaces that help us match generated messages

type CollectionElem interface {
	DefaultDetails() *pgcol.CollectionDetails
	DataProto() proto.Message
	LoadData([]interface{})
	//Force data to come back as a slice
	InterfaceSlice() []interface{}
	ProtoSlice() []proto.Message
	CollectionMessageSlice() []CollectionMessage
	DataIsCollectionMessage() bool
	proto.Message
}

type CollectionMessage interface {
	//Map of collection keys to empty CollectionElem's
	DefaultCollectionMap() map[string]CollectionElem
	//List of collection keys
	CollectionKeys() []string
	//Load data into a collection by it's key
	LoadCollection(string, []interface{}) error
	//CollectionElem for a particular key
	CollectionElem(string) CollectionElem
	//Get slice of data currently loaded into a particular collection as interface{}
	CollectionInterfaceSlice(string) []interface{}
	//Get slice of data currently loaded into a particular collection as proto.Message
	CollectionProtoSlice(string) []proto.Message
	SetCollectionParentKeyData(interface{}, string)
	SetCollectionKeyData(interface{}, interface{}, string)
	SetCollectionKeyDataFromParent(interface{}, string) string
	CollectionParentKeyData(string) interface{}
	CollectionParentKeyIsDefault(string) bool
	CollectionKeyData(interface{}, string) interface{}
	CollectionKeyIsDefault(interface{}, string) bool
	ProtoBelongsToCollection(interface{}, string) bool
	proto.Message
}

type CollectionGap interface {
	GapBridge() CollectionMessage
}

type CollectionWriterData interface {
	DataMessage() proto.Message
	ParentMessage() CollectionMessage
}
