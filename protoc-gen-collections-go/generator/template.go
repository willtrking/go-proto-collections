package generator

import "text/template"

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

func (p *{{.ParentGoName}}) LoadCollection(collection string, data []interface{}) error {

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

//Get the data for a collection from the parent, which is this protobuf
func (p *{{.ParentGoName}}) CollectionParentKeyData(collection string) interface{} {
	switch collection {
	{{ range .Collections }}
	case "{{.CollectionName}}":
		return p.{{.ParentGoKey}}
	{{ end }}
	default:
		return nil
	}
}

//Get the data for a collection from an element that could be in the collection
func (p *{{.ParentGoName}}) CollectionKeyData(pb interface{}, collection string) interface{} {
	switch collection {
	{{ range .Collections }}
	case "{{.CollectionName}}":
		if pb == nil {
			return nil 
		} else {
			return pb.(*{{.CollectionDataTypeGoName}}).{{.CollectionGoKey}}
		}
	{{ end }}
	default:
		return nil
	}
}

func (p *{{.ParentGoName}}) ProtoBelongsToCollection(pb interface{}, collection string) bool {
	switch collection {
	{{ range .Collections }}
	case "{{.CollectionName}}":

		if c, ok := pb.(*{{.CollectionDataTypeGoName}}); ok {
			return c.{{.CollectionGoKey}} == p.{{.ParentGoKey}}
		} else {
			return false
		}
	{{ end }}
	default:
		return false
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

func (c *{{.CollectionGoType}}) LoadData(data []interface{}) {

	c.{{.CollectionDetailsGoName}} = c.DefaultDetails()

	dL := len(data)

	if dL > 0 {

		c.{{.CollectionDataGoName}} = make([]*{{.CollectionDataTypeGoName}}, dL)

		for i, v := range data {
			if v != nil {
				c.{{.CollectionDataGoName}}[i] = v.(*{{.CollectionDataTypeGoName}})
			}
		}

		if len(c.{{.CollectionDataGoName}}) != dL {
			d := make([]*{{.CollectionDataTypeGoName}}, len(c.{{.CollectionDataGoName}}))
			copy(d, c.{{.CollectionDataGoName}})
			c.{{.CollectionDataGoName}} = d
		}

		c.{{.CollectionDetailsGoName}}.Loaded = true

	} else {
		c.{{.CollectionDetailsGoName}}.Loaded = true
	}

}

func (c *{{.CollectionGoType}}) DataSlice() []interface{} {
	if c.Data != nil {
		s := make([]interface{},len(c.Data))
		for idx, d := range c.Data {
			s[idx] = d
		}
		return s
	} else {
		var s []interface{}
		return s
	}
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

func (c *{{.CollectionGoType}}) LoadData(data []interface{}) {

	c.{{.CollectionDetailsGoName}} = c.DefaultDetails()
	c.{{.CollectionDetailsGoName}}.Loaded = false

	if len(data) > 0 {
		for _, v := range data {
			if v != nil {
				c.{{.CollectionDataGoName}} = v.(*{{.CollectionDataTypeGoName}})
				c.{{.CollectionDetailsGoName}}.Loaded = true
				break
			}
		}

		if c.{{.CollectionDetailsGoName}}.Loaded != true {
			c.{{.CollectionDataGoName}} = &{{.CollectionDataTypeGoName}}{}
			c.{{.CollectionDetailsGoName}}.Loaded = true
		}
		

	} else {
		c.{{.CollectionDataGoName}} = &{{.CollectionDataTypeGoName}}{}
		c.{{.CollectionDetailsGoName}}.Loaded = true
	}

}

func (c *{{.CollectionGoType}}) DataSlice() []interface{} {
	if c.Data != nil {
		s := make([]interface{},1)
		s[0] = c.Data
		return s
	} else {
		var s []interface{}
		return s
	}
}
`))

var collectionGapTemplate = template.Must(template.New("collectonGap").Parse(`func (g *{{.GapGoName}}) GapBridge() helpers.CollectionMessage {
	return g.{{.GapGoAttribute}}
}

func (g *{{.GapGoName}}) DefaultCollectionMap() map[string]helpers.CollectionElem {
	return g.{{.GapGoAttribute}}.DefaultCollectionMap()
}

func (g *{{.GapGoName}}) LoadCollection(collection string, data []interface{}) error {
	return g.{{.GapGoAttribute}}.LoadCollection(collection, data)
}

func (g *{{.GapGoName}}) CollectionDataSlice(collection string) []interface{} {
	return g.{{.GapGoAttribute}}.CollectionDataSlice(collection)
}

func (g *{{.GapGoName}}) CollectionParentKeyData(collection string) interface{} {
	return g.{{.GapGoAttribute}}.CollectionParentKeyData(collection)
}

func (g *{{.GapGoName}}) CollectionKeyData(pb interface{}, collection string) interface{} {
	return g.{{.GapGoAttribute}}.CollectionKeyData(pb, collection)
}

func (g *{{.GapGoName}}) ProtoBelongsToCollection(pb interface{}, collection string) bool {
	return g.{{.GapGoAttribute}}.ProtoBelongsToCollection(pb, collection)
}
`))

var collectionLoaderTemplate = template.Must(template.New("collectonLoader").Parse(`// Code generated by protoc-gen-collections-go
// DO NOT EDIT!

package {{.GoPackage}}

import (
	"fmt"
	"sync"
	"errors"
	"github.com/willtrking/go-proto-collections/runtime"
)



////////////////////////////////////////////////////////////////////////////////
///// IMPLEMENT THIS INTERFACE!!
////////////////////////////////////////////////////////////////////////////////

type {{.CollectionDataTypeGoName}}Loader interface {
	{{ range .CLTypes }}
	From{{.CollectionGoKey}}(string, []{{.CollectionKeyGoType}}, chan []*{{.CollectionDataTypeGoName}})
	{{ end }}	
}


////////////////////////////////////////////////////////////////////////////////
///// CALL THIS WITH THE IMPLEMENTATION OF ABOVE INTERFACE!!
////////////////////////////////////////////////////////////////////////////////

func Register{{.CollectionDataTypeGoName}}_Loader(r *runtime.CollectionRegistry, l {{.CollectionDataTypeGoName}}Loader) {
	{{ range .CLRegisterTypes }}
	r.RegisterLoader(&{{.CollectionGoType}}{}, New{{.CollectionDataTypeGoName}}_Loader(l))
	{{ end }}
}





type {{.CollectionDataTypeGoName}}_Loader struct {
	dataKey  string
	loadLock sync.Mutex
	waitLock sync.Mutex
	loading  bool
	loaded   bool
	once     sync.Once
	Data     []*{{.CollectionDataTypeGoName}}
	Loader   {{.CollectionDataTypeGoName}}Loader
}

func (c *{{.CollectionDataTypeGoName}}_Loader) DataKey() string { return c.dataKey }
func (c *{{.CollectionDataTypeGoName}}_Loader) Loading() bool { return c.loading }
func (c *{{.CollectionDataTypeGoName}}_Loader) Loaded() bool  { return c.loaded }
func (c *{{.CollectionDataTypeGoName}}_Loader) Wait() {
	c.waitLock.Lock()
	defer c.waitLock.Unlock()
}

func (c *{{.CollectionDataTypeGoName}}_Loader) SetDataKey(key string) error {

	switch key {
	{{ range .CLTypes }}
	case "{{.CollectionKey}}":
		c.dataKey = key
		return nil
	{{ end }}
	default:
		return errors.New(fmt.Sprintf("Unknown key %s",key))
	}

	return nil
}

func (c *{{.CollectionDataTypeGoName}}_Loader) DataSlice() []interface{} {

	dL := len(c.Data)

	if dL > 0 {
		f := make([]interface{}, dL)

		for i, dI := range c.Data {
			func(y interface{}) {
				f[i] = y
			}(&*dI)
		}

		return f
	} else {
		return nil
	}

}


func (c *{{.CollectionDataTypeGoName}}_Loader) Load(from []interface{}) {
	//Need to acquire a lock here so we wait until the first call is done
	//Provides safety for DataSlice()
	c.loadLock.Lock()
	defer c.loadLock.Unlock()

	loader := func() {
		c.loading = true
		defer func() { c.loading = false }()
		defer func() { c.loaded = true }()
		defer c.waitLock.Unlock()

		switch c.dataKey {
		{{ range .CLTypes }}
		case "{{.CollectionKey}}":
			var a []{{.CollectionKeyGoType}}
			lc := make(chan []*{{.CollectionDataTypeGoName}},1)

			for _, f := range from {
				if f != nil {
					a = append(a,f.({{.CollectionKeyGoType}}))
				}
			}
			
			if a != nil && len(a) > 0 {
				go c.Loader.From{{.CollectionGoKey}}("{{.CollectionKey}}",a,lc)
				c.Data = <-lc
			}
		{{ end }}
		default:
			c.Data = nil	
		}

	}

	c.once.Do(loader)

}

func New{{.CollectionDataTypeGoName}}_Loader(l {{.CollectionDataTypeGoName}}Loader) func(string) (runtime.CollectionLoader,error) {

	return func(k string) (runtime.CollectionLoader,error) {
		f := &{{.CollectionDataTypeGoName}}_Loader{
			dataKey:  "",
			loadLock: sync.Mutex{},
			waitLock: sync.Mutex{},
			loading:  false,
			loaded:   false,
			once:     sync.Once{},
			Data:     []*{{.CollectionDataTypeGoName}}{},
			Loader:   l,
		}

		kErr := f.SetDataKey(k)
		if kErr != nil {
			return nil,kErr
		}

		f.waitLock.Lock()

		return f,nil
	}
}

`))
