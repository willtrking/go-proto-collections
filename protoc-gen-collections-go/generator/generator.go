package generator

import (
	"bytes"
	"fmt"
	"go/format"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	pdescriptor "github.com/golang/protobuf/protoc-gen-go/descriptor"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
	"github.com/willtrking/go-proto-collections/protoc-gen-collections-go/descriptor"
	pcol "github.com/willtrking/go-proto-collections/protocollections"
)

func upperFirst(s string) string {
	if s == "" {
		return ""
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToUpper(r)) + s[n:]
}

func lowerFirst(s string) string {
	if s == "" {
		return ""
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToLower(r)) + s[n:]
}

func collectionDataTypeGoName(dataField *pdescriptor.FieldDescriptorProto, file *descriptor.File) string {
	dataType := strings.Replace(dataField.GetTypeName(), "."+file.GetPackage()+".", "", 1)
	return upperFirst(dataType)

}

func MsgAttr(attr string, msg *descriptor.Message) *pdescriptor.FieldDescriptorProto {
	for _, field := range msg.Field {
		if field.GetName() == attr {
			//Found it!
			return field
		}
	}
	return nil
}

type GeneratorFile struct {
	GoPackage      string
	Source         string
	File           *descriptor.File
	ParentMessages []*ParentMessage
	CollectionGaps []*CollectionGap
}

type CollectionGap struct {
	GapGoName      string
	GapGoAttribute string
}

type ParentMessage struct {
	ParentGoName           string
	ParentCollectionGoType string
	ParentGoCollectionAttr string

	Collections []*CollectionMessage
}

type CollectionMessage struct {
	ParentGoCollectionAttr   string
	CollectionName           string
	CollectionGoName         string
	CollectionGoType         string
	CollectionDetailsGoName  string
	CollectionDataGoName     string
	CollectionDataTypeGoName string
	ParentKey                string
	ParentGoKey              string
	CollectionKey            string
	CollectionGoKey          string
	MaxResults               uint64
	NextRPC                  string
	Listable                 bool
}

type Loader struct {
	GoPackage                string
	CollectionDataTypeGoName string
	CLTypes                  []*LoaderType
	CLRegisterTypes          []*LoaderType
	foundCLTypes             map[string]bool
}

type LoaderType struct {
	CollectionDataTypeGoName string
	CollectionGoType         string
	CollectionKey            string
	CollectionGoKey          string
	CollectionKeyGoType      string
}

type Generator struct {
	Targets       map[string]*GeneratorFile
	registry      *descriptor.Registry
	targetLoaders map[string]*Loader
}

func (g *Generator) addLoader(col *CollectionMessage, collectionKeyAttr *pdescriptor.FieldDescriptorProto, collectionKeyMessage *descriptor.Message) {

	if _, ok := g.targetLoaders[col.CollectionDataTypeGoName]; !ok {
		//Dont have it
		g.targetLoaders[col.CollectionDataTypeGoName] = &Loader{
			GoPackage:                collectionKeyMessage.File.GoPkg.Name,
			CollectionDataTypeGoName: col.CollectionDataTypeGoName,
			foundCLTypes:             make(map[string]bool),
		}
	}

	//Haven't added stuff for the CollectionGoKey of this particular loader yet

	cKeyGoType := descriptor.GetGoBaseType(collectionKeyAttr.Type, collectionKeyMessage.File)
	if cKeyGoType == "" {
		if collectionKeyAttr.Type != nil {
			if *collectionKeyAttr.Type == pdescriptor.FieldDescriptorProto_TYPE_MESSAGE {
				cKeyMessage, _ := g.registry.LookupMsg("", collectionKeyAttr.GetTypeName())

				if cKeyMessage == nil {
					glog.Fatalf(fmt.Sprintf("Can't locate concrete go type for collectionKey %s, unknown message type %s", col.CollectionKey, collectionKeyAttr.GetTypeName()))
				}

				cKeyGoType = collectionDataTypeGoName(collectionKeyAttr, collectionKeyMessage.File)

				if cKeyGoType == "" {
					glog.Fatalf(fmt.Sprintf("Can't locate concrete go type for collectionKey %s, must be a message type or a concrete Go type", col.CollectionKey))
				}

			} else {
				glog.Fatalf(fmt.Sprintf("Can't locate concrete go type for collectionKey %s, must be a message type or a concrete Go type", col.CollectionKey))
			}
		} else {
			glog.Fatalf(fmt.Sprintf("Can't locate concrete go type for collectionKey %s, must be a message type or a concrete Go type", col.CollectionKey))
		}

	}

	l := &LoaderType{
		CollectionDataTypeGoName: col.CollectionDataTypeGoName,
		CollectionGoType:         col.CollectionGoType,
		CollectionKey:            col.CollectionKey,
		CollectionGoKey:          col.CollectionGoKey,
		CollectionKeyGoType:      cKeyGoType,
	}

	g.targetLoaders[col.CollectionDataTypeGoName].CLRegisterTypes = append(g.targetLoaders[col.CollectionDataTypeGoName].CLRegisterTypes, l)

	dedupKey := col.CollectionDataTypeGoName + " " + col.CollectionGoKey

	if _, ok := g.targetLoaders[col.CollectionDataTypeGoName].foundCLTypes[dedupKey]; !ok {
		g.targetLoaders[col.CollectionDataTypeGoName].CLTypes = append(g.targetLoaders[col.CollectionDataTypeGoName].CLTypes, l)
		g.targetLoaders[col.CollectionDataTypeGoName].foundCLTypes[dedupKey] = true
	}

}

func (g *Generator) AddGap(file *descriptor.File, gap *descriptor.Message, gapAttr string) {
	fileName := file.GetName()
	if _, ok := g.Targets[fileName]; !ok {
		//Dont have it
		g.Targets[fileName] = &GeneratorFile{
			GoPackage: file.GoPkg.Name,
			Source:    fileName,
			File:      file,
		}
	}

	gapMessage := &CollectionGap{
		GapGoName:      upperFirst(gap.GetName()),
		GapGoAttribute: upperFirst(gapAttr),
	}

	g.Targets[fileName].CollectionGaps = append(g.Targets[fileName].CollectionGaps, gapMessage)

}

func (g *Generator) AddTarget(file *descriptor.File, parent *descriptor.Message, parentAttr string, col *pdescriptor.DescriptorProto) {
	fileName := file.GetName()

	if _, ok := g.Targets[fileName]; !ok {
		//Dont have it
		g.Targets[fileName] = &GeneratorFile{
			GoPackage: file.GoPkg.Name,
			Source:    fileName,
			File:      file,
		}
	}

	parentMessage := &ParentMessage{
		ParentGoName:           upperFirst(parent.GetName()),
		ParentCollectionGoType: upperFirst(parent.GetName()) + "_" + upperFirst(col.GetName()),
		ParentGoCollectionAttr: upperFirst(parentAttr),
	}

	//Look for messages nested in our collection type
	//These form our individual collections
	//Each must have 2 elements, one of which is a CollectionDetails
	//Each must have the required options, which are
	// - parentKey
	// - collectionKey
	//Optional params are
	// - nextRPC
	// - maxResults
	//Each must be used once in its collection type
	for _, colNested := range col.NestedType {
		if len(colNested.Field) == 2 {

			var collectionField *pdescriptor.FieldDescriptorProto
			var dataField *pdescriptor.FieldDescriptorProto
			for _, field := range colNested.Field {
				if strings.Contains(field.GetTypeName(), "CollectionDetails") {
					collectionField = field
				} else {
					dataField = field
				}

			}

			if colNested.Options != nil && collectionField != nil && dataField != nil {

				//Make sure our nested type is used once in our collection type

				nestedName := "." + file.GetPackage() + "." + parent.GetName() + "." + col.GetName() + "." + colNested.GetName()

				usedOnce := false
				var colNestedField *pdescriptor.FieldDescriptorProto

				for _, field := range col.Field {

					if nestedName == field.GetTypeName() {
						if !usedOnce {
							colNestedField = field
							usedOnce = true
						} else {
							usedOnce = false
							break
						}
					}
				}

				//Only used once
				if usedOnce && colNestedField != nil {

					dataMessage, _ := g.registry.LookupMsg("", dataField.GetTypeName())

					if dataMessage == nil {
						glog.Fatalf(fmt.Sprintf("Collection data field in collection %s in parent %s must be a known message, got %s of type %#v",
							colNested.GetName(), parent.GetName(), dataField.GetTypeName(), dataField.Type))
					}

					collectionMessage := &CollectionMessage{
						ParentGoCollectionAttr:   upperFirst(parentAttr),
						CollectionName:           colNestedField.GetName(),
						CollectionGoName:         upperFirst(colNestedField.GetName()),
						CollectionGoType:         upperFirst(parent.GetName()) + "_" + upperFirst(col.GetName()) + "_" + upperFirst(colNested.GetName()),
						CollectionDetailsGoName:  upperFirst(collectionField.GetName()),
						CollectionDataGoName:     upperFirst(dataField.GetName()),
						CollectionDataTypeGoName: collectionDataTypeGoName(dataField, file),
						Listable:                 dataField.GetLabel() == pdescriptor.FieldDescriptorProto_LABEL_REPEATED,
					}

					parentKey, err := proto.GetExtension(colNested.Options, pcol.E_ParentKey)
					if err != nil || parentKey == nil {
						//Error getting parent key, error
						glog.Fatalf(fmt.Sprintf("Found likely collection %s, but missing parentKey option", collectionMessage.CollectionGoType))
					}

					pKey := parentKey.(*string)

					if pKey == nil {
						glog.Fatalf(fmt.Sprintf("Found likely collection %s, but missing parentKey option", collectionMessage.CollectionGoType))
					}

					collectionMessage.ParentKey = strings.TrimSpace(*pKey)
					collectionMessage.ParentGoKey = upperFirst(collectionMessage.ParentKey)

					if MsgAttr(collectionMessage.ParentKey, parent) == nil {
						glog.Fatalf(fmt.Sprintf("Found likely collection %s, but parentKey option doesn't exist in parent %s!", collectionMessage.CollectionGoType, parent.GetName()))
					}

					collectionKey, err := proto.GetExtension(colNested.Options, pcol.E_CollectionKey)
					if err != nil || collectionKey == nil {
						//Error getting parent key, error
						glog.Fatalf(fmt.Sprintf("Found likely collection %s, but missing collectionKey option", collectionMessage.CollectionGoType))
					}

					cKey := collectionKey.(*string)

					if cKey == nil {
						glog.Fatalf(fmt.Sprintf("Found likely collection %s, but missing collectionKey option", collectionMessage.CollectionGoType))
					}

					collectionMessage.CollectionKey = *cKey
					collectionMessage.CollectionGoKey = upperFirst(collectionMessage.CollectionKey)

					collectionKeyAttr := MsgAttr(collectionMessage.CollectionKey, dataMessage)
					if collectionKeyAttr == nil {
						glog.Fatalf(fmt.Sprintf("Found likely collection %s, but collectionKey option doesn't exist in data field type %s!", collectionMessage.CollectionGoType, dataField.GetTypeName()))
					}

					//dataMessage

					//If we're listable check for max results, otherwise always 1
					if collectionMessage.Listable {
						//Check if we defined it, otherwise default to 0 (unlimited results)
						maxResults, err := proto.GetExtension(colNested.Options, pcol.E_MaxResults)
						if err == nil && maxResults != nil {
							maxR := maxResults.(*uint64)

							if maxR == nil {
								glog.Fatalf(fmt.Sprintf("Found maxResults in %s, but its empty!", collectionMessage.CollectionGoType))
							}

							collectionMessage.MaxResults = *maxR

						} else {
							collectionMessage.MaxResults = 0
						}

					} else {
						collectionMessage.MaxResults = 1
					}

					//Now look for NextRPC, if we have if
					nextRPC, err := proto.GetExtension(colNested.Options, pcol.E_NextRPC)
					if err == nil && nextRPC != nil {
						nextR := nextRPC.(*string)

						if nextR == nil {
							glog.Fatalf(fmt.Sprintf("Found nextRPC in %s, but its empty!", collectionMessage.CollectionGoType))
						}

						collectionMessage.NextRPC = *nextR

					} else {
						//Otherwise just empty
						collectionMessage.NextRPC = ""
					}

					g.addLoader(collectionMessage, collectionKeyAttr, dataMessage)
					parentMessage.Collections = append(parentMessage.Collections, collectionMessage)
				}

			}
		}
	}

	if len(parentMessage.Collections) > 0 {
		//Have at least 1 collection, so add to our file
		g.Targets[fileName].ParentMessages = append(g.Targets[fileName].ParentMessages, parentMessage)
	}

}

func (g *Generator) Generate() ([]*plugin.CodeGeneratorResponse_File, error) {
	var files []*plugin.CodeGeneratorResponse_File

	for _, target := range g.Targets {
		var toWrite bytes.Buffer
		wroteHeader := false
		didWrite := false
		if len(target.ParentMessages) > 0 {

			if !wroteHeader {
				fileHeaderTemplate.Execute(&toWrite, target)
				toWrite.WriteRune('\n')
				wroteHeader = true
			}

			for _, targetParent := range target.ParentMessages {
				parentMessageTemplate.Execute(&toWrite, targetParent)
				toWrite.WriteRune('\n')
				for _, targetCollection := range targetParent.Collections {
					if targetCollection.Listable {
						listableCollectionTemplate.Execute(&toWrite, targetCollection)
						toWrite.WriteRune('\n')
					} else {
						collectionTemplate.Execute(&toWrite, targetCollection)
						toWrite.WriteRune('\n')
					}
				}
			}

			didWrite = true

		}

		if len(target.CollectionGaps) > 0 {
			if !wroteHeader {
				fileHeaderTemplate.Execute(&toWrite, target)
				toWrite.WriteRune('\n')
				wroteHeader = true
			}

			for _, targetGap := range target.CollectionGaps {
				collectionGapTemplate.Execute(&toWrite, targetGap)
				toWrite.WriteRune('\n')
			}

			didWrite = true

		}

		if didWrite {
			formatted, err := format.Source(toWrite.Bytes())

			if err != nil {
				glog.Errorf("%v", err)
				return nil, err
			}

			name := target.File.GetName()
			ext := filepath.Ext(name)
			base := strings.TrimSuffix(name, ext)
			output := fmt.Sprintf("%s.pb.col.go", base)

			files = append(files, &plugin.CodeGeneratorResponse_File{
				Name:    proto.String(output),
				Content: proto.String(string(formatted)),
			})
		}

	}

	for loaderName, loader := range g.targetLoaders {

		if len(loader.CLTypes) > 0 {

			var toWrite bytes.Buffer

			collectionLoaderTemplate.Execute(&toWrite, loader)
			formatted, err := format.Source(toWrite.Bytes())

			if err != nil {
				glog.Errorf("%v", err)
				return nil, err
			}

			output := fmt.Sprintf("%s.coloader.go", lowerFirst(loaderName))

			files = append(files, &plugin.CodeGeneratorResponse_File{
				Name:    proto.String(output),
				Content: proto.String(string(formatted)),
			})

		}
	}

	return files, nil
}

func NewGenerator(r *descriptor.Registry) *Generator {
	return &Generator{
		Targets:       make(map[string]*GeneratorFile),
		targetLoaders: make(map[string]*Loader),
		registry:      r,
	}
}
