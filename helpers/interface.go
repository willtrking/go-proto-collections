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
	DefaultCollectionMap() map[string]CollectionElem
	CollectionKeys() []string
	LoadCollection(string, []interface{}) error
	CollectionElem(string) CollectionElem
	CollectionInterfaceSlice(string) []interface{}
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
