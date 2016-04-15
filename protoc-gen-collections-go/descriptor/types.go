package descriptor

import (
	"fmt"
	"strings"

	descriptor "github.com/golang/protobuf/protoc-gen-go/descriptor"
)

// GoPackage represents a golang package
type GoPackage struct {
	// Path is the package path to the package.
	Path string
	// Name is the package name of the package
	Name string
	// Alias is an alias of the package unique within the current invokation of grpc-gateway generator.
	Alias string
}

// Standard returns whether the import is a golang standard package.
func (p GoPackage) Standard() bool {
	return !strings.Contains(p.Path, ".")
}

// String returns a string representation of this package in the form of import line in golang.
func (p GoPackage) String() string {
	if p.Alias == "" {
		return fmt.Sprintf("%q", p.Path)
	}
	return fmt.Sprintf("%s %q", p.Alias, p.Path)
}

// File wraps descriptor.FileDescriptorProto for richer features.
type File struct {
	*descriptor.FileDescriptorProto
	// GoPkg is the go package of the go file generated from this file..
	GoPkg GoPackage
	// Messages is the list of messages defined in this file.
	Messages []*Message
}

// proto2 determines if the syntax of the file is proto2.
func (f *File) proto2() bool {
	return f.Syntax == nil || f.GetSyntax() == "proto2"
}

// Message describes a protocol buffer message types
type Message struct {
	// File is the file where the message is defined
	File *File
	// Outers is a list of outer messages if this message is a nested type.
	Outers []string
	*descriptor.DescriptorProto
	Fields []*Field
}

// FQMN returns a fully qualified message name of this message.
func (m *Message) FQMN() string {
	components := []string{""}
	if m.File.Package != nil {
		components = append(components, m.File.GetPackage())
	}
	components = append(components, m.Outers...)
	components = append(components, m.GetName())
	return strings.Join(components, ".")
}

// GoType returns a go type name for the message type.
// It prefixes the type name with the package alias if
// its belonging package is not "currentPackage".
func (m *Message) GoType(currentPackage string) string {
	var components []string
	components = append(components, m.Outers...)
	components = append(components, m.GetName())

	name := strings.Join(components, "_")
	if m.File.GoPkg.Path == currentPackage {
		return name
	}
	pkg := m.File.GoPkg.Name
	if alias := m.File.GoPkg.Alias; alias != "" {
		pkg = alias
	}
	return fmt.Sprintf("%s.%s", pkg, name)
}

// Field wraps descriptor.FieldDescriptorProto for richer features.
type Field struct {
	// Message is the message type which this field belongs to.
	Message *Message
	// FieldMessage is the message type of the field.
	FieldMessage *Message
	*descriptor.FieldDescriptorProto
}

func GetGoBaseType(t *descriptor.FieldDescriptorProto_Type, f *File) string {
	if t == nil {
		return ""
	}

	if f.proto2() {
		if st, ok := goProto2BaseType[*t]; ok {
			return st
		}
	} else {
		if st, ok := goProto3BaseType[*t]; ok {
			return st
		}
	}

	return ""
}

var goProto3BaseType = map[descriptor.FieldDescriptorProto_Type]string{
	descriptor.FieldDescriptorProto_TYPE_DOUBLE:   "float64",
	descriptor.FieldDescriptorProto_TYPE_FLOAT:    "float32",
	descriptor.FieldDescriptorProto_TYPE_INT64:    "int64",
	descriptor.FieldDescriptorProto_TYPE_UINT64:   "uint64",
	descriptor.FieldDescriptorProto_TYPE_INT32:    "int32",
	descriptor.FieldDescriptorProto_TYPE_UINT32:   "uint32",
	descriptor.FieldDescriptorProto_TYPE_FIXED64:  "uint64",
	descriptor.FieldDescriptorProto_TYPE_FIXED32:  "uint32",
	descriptor.FieldDescriptorProto_TYPE_BOOL:     "bool",
	descriptor.FieldDescriptorProto_TYPE_STRING:   "string",
	descriptor.FieldDescriptorProto_TYPE_BYTES:    "[]byte",
	descriptor.FieldDescriptorProto_TYPE_SFIXED32: "int32",
	descriptor.FieldDescriptorProto_TYPE_SFIXED64: "int64",
	descriptor.FieldDescriptorProto_TYPE_SINT32:   "int32",
	descriptor.FieldDescriptorProto_TYPE_SINT64:   "int64",
}

var goProto2BaseType = map[descriptor.FieldDescriptorProto_Type]string{
	descriptor.FieldDescriptorProto_TYPE_DOUBLE:   "*float64",
	descriptor.FieldDescriptorProto_TYPE_FLOAT:    "*float32",
	descriptor.FieldDescriptorProto_TYPE_INT64:    "*int64",
	descriptor.FieldDescriptorProto_TYPE_UINT64:   "*uint64",
	descriptor.FieldDescriptorProto_TYPE_INT32:    "*int32",
	descriptor.FieldDescriptorProto_TYPE_UINT32:   "*uint32",
	descriptor.FieldDescriptorProto_TYPE_FIXED64:  "*uint64",
	descriptor.FieldDescriptorProto_TYPE_FIXED32:  "*uint32",
	descriptor.FieldDescriptorProto_TYPE_BOOL:     "*bool",
	descriptor.FieldDescriptorProto_TYPE_STRING:   "*string",
	descriptor.FieldDescriptorProto_TYPE_BYTES:    "[]byte",
	descriptor.FieldDescriptorProto_TYPE_SFIXED32: "*int32",
	descriptor.FieldDescriptorProto_TYPE_SFIXED64: "*int64",
	descriptor.FieldDescriptorProto_TYPE_SINT32:   "*int32",
	descriptor.FieldDescriptorProto_TYPE_SINT64:   "*int64",
}
