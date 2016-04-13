package generator

import "html/template"

var fileHeaderTemplate = template.Must(template.New("fileHeader").Parse(`// Code generated by protoc-gen-collections-go
// source: {{.Source}}
// DO NOT EDIT!

package {{.GoPackage}}
import (
	"fmt"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/willtrking/go-proto-collections/helpers"
	pgcol "github.com/willtrking/go-proto-collections/protocollections"

)
`))

var parentMessageTemplate = template.Must(template.New("parentMessage").Parse(`func (p *{{.ParentGoName}}) DefaultCollectionMap() map[string]helpers.CollectionElem {
	m := make(map[string]helpers.CollectionElem)


	{{ range .Collections }}
	m["{{.CollectionName}}"] = &{{.CollectionGoType}}{}
	{{ end }}

	return m
}

func (p *{{.ParentGoName}}) LoadCollection(collection string, data interface{}) error {

	if p.{{.ParentGoCollectionAttr}} == nil {
		p.{{.ParentGoCollectionAttr}} = &{{.ParentCollectionGoType}}{}
	}

	switch collection {
	{{ range .Collections }}
	case "{{.CollectionName}}":
		p.{{.ParentGoCollectionAttr}}.{{.CollectionGoName}} = &{{.CollectionGoType}}{}
		p.{{.ParentGoCollectionAttr}}.{{.CollectionGoName}}.LoadData(data)
		return nil	
	{{ end }}
	default:
		return errors.New(fmt.Sprintf("Unknown collection %s", collection))
	}

}

func (p *{{.ParentGoName}}) CollectionDataSlice(collection string) []interface{} {
	if p.{{.ParentGoCollectionAttr}} == nil {
		return nil
	}

	switch collection {
	{{ range .Collections }}
	case "{{.CollectionName}}":
		if p.{{.ParentGoCollectionAttr}}.{{.CollectionGoName}} == nil {
			return nil
		} else {
			return p.{{.ParentGoCollectionAttr}}.{{.CollectionGoName}}.DataSlice()
		}
	{{ end }}
	default:
		return nil
	}

}
`))

var listableCollectionTemplate = template.Must(template.New("listableCollection").Parse(`func (*{{.CollectionGoType}}) DefaultDetails() *pgcol.CollectionDetails {
	return &pgcol.CollectionDetails{
		ParentKey:     "{{.ParentKey}}",
		CollectionKey: "{{.CollectionKey}}",
		Listable:      true,
		Loaded:        false,
		MaxResults:     {{.MaxResults}},
		NextRPC:       "{{.NextRPC}}",
		NextBody:      nil,
	}
}

func (*{{.CollectionGoType}}) DataProto() proto.Message {
	return &{{.CollectionDataTypeGoName}}{}
}

func (c *{{.CollectionGoType}}) LoadData(data interface{}) {

	c.{{.CollectionDetailsGoName}} = c.DefaultDetails()

	if data != nil {
		conv, len, _ := helpers.EnsureSlice(data)

		c.{{.CollectionDataGoName}} = make([]*{{.CollectionDataTypeGoName}}, len)

		for i, v := range conv {
			c.{{.CollectionDataGoName}}[i] = v.(*{{.CollectionDataTypeGoName}})
		}

		c.{{.CollectionDetailsGoName}}.Loaded = true

	} else {
		c.{{.CollectionDetailsGoName}}.Loaded = true
	}

}

func (c *{{.CollectionGoType}}) DataSlice() []interface{} {
	conv, _, _ := helpers.EnsureSlice(c.{{.CollectionDataGoName}})
	return conv
}
`))

var collectionTemplate = template.Must(template.New("collection").Parse(`func (*{{.CollectionGoType}}) DefaultDetails() *pgcol.CollectionDetails {
	return &pgcol.CollectionDetails{
		ParentKey:     "{{.ParentKey}}",
		CollectionKey: "{{.CollectionKey}}",
		Listable:      false,
		Loaded:        false,
		MaxResults:     {{.MaxResults}},
		NextRPC:       "{{.NextRPC}}",
		NextBody:      nil,
	}
}

func (*{{.CollectionGoType}}) DataProto() proto.Message {
	return &{{.CollectionDataTypeGoName}}{}
}

func (c *{{.CollectionGoType}}) LoadData(data interface{}) {

	c.{{.CollectionDetailsGoName}} = c.DefaultDetails()

	r, _ := helpers.EnsureNotSlice(data)

	if r != nil {
		c.{{.CollectionDataGoName}} = r.(*{{.CollectionDataTypeGoName}})
		c.{{.CollectionDetailsGoName}}.Loaded = true

	} else {
		c.{{.CollectionDataGoName}} = &{{.CollectionDataTypeGoName}}{}
		c.{{.CollectionDetailsGoName}}.Loaded = true
	}

}

func (c *{{.CollectionGoType}}) DataSlice() []interface{} {
	conv, _, _ := helpers.EnsureSlice(c.{{.CollectionDataGoName}})
	return conv
}
`))
