package main

import (
	"flag"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
	"github.com/willtrking/go-proto-collections/protoc-gen-collections-go/descriptor"
	"github.com/willtrking/go-proto-collections/protoc-gen-collections-go/generator"
)

var (
	importPrefix = flag.String("import_prefix", "", "prefix to be added to go package paths for imported proto files")
)

func parseReq(r io.Reader) (*plugin.CodeGeneratorRequest, error) {
	glog.V(1).Info("Parsing code generator request")
	input, err := ioutil.ReadAll(r)
	if err != nil {
		glog.Errorf("Failed to read code generator request: %v", err)
		return nil, err
	}
	req := new(plugin.CodeGeneratorRequest)
	if err = proto.Unmarshal(input, req); err != nil {
		glog.Errorf("Failed to unmarshal code generator request: %v", err)
		return nil, err
	}
	glog.V(1).Info("Parsed code generator request")
	return req, nil
}

func main() {
	flag.Parse()
	defer glog.Flush()

	reg := descriptor.NewRegistry()

	glog.V(1).Info("Processing code generator request")
	req, err := parseReq(os.Stdin)

	if err != nil {
		glog.Fatal(err)
	}
	if req.Parameter != nil {
		for _, p := range strings.Split(req.GetParameter(), ",") {
			spec := strings.SplitN(p, "=", 2)
			if len(spec) == 1 {
				if err := flag.CommandLine.Set(spec[0], ""); err != nil {
					glog.Fatalf("Cannot set flag %s", p)
				}
				continue
			}
			name, value := spec[0], spec[1]
			if strings.HasPrefix(name, "M") {
				reg.AddPkgMap(name[1:], value)
				continue
			}
			if err := flag.CommandLine.Set(name, value); err != nil {
				glog.Fatalf("Cannot set flag %s", p)
			}
		}
	}

	reg.SetPrefix(*importPrefix)
	if err := reg.Load(req); err != nil {
		emitError(err)
		return
	}

	var targets []*descriptor.File
	for _, target := range req.FileToGenerate {
		f, err := reg.LookupFile(target)
		//fmt.Println(f)
		if err != nil {
			glog.Fatal(err)
		}
		targets = append(targets, f)
	}

	g := generator.NewGenerator()
	//Look for our format, as follows
	// - Messages named Collections
	// - Nested (defined) inside of 1 message
	// - Used inside that same message ONCE

	for _, file := range targets {
		for _, msg := range file.Messages {
			for _, nested := range msg.NestedType {
				if nested.GetName() == "Collections" {
					//Check our name
					msgName := "." + file.GetPackage() + "." + msg.GetName() + ".Collections"
					//We have collections
					haveOne := false
					fieldName := ""
					for _, field := range msg.Field {

						if msgName == field.GetTypeName() {
							if !haveOne {
								fieldName = field.GetName()
								haveOne = true
							} else {
								haveOne = false
								break
							}
						}
					}

					if haveOne {
						g.AddTarget(file, msg, fieldName, nested)
					}

				}
			}
		}
	}

	out, err := g.Generate()
	if err != nil {
		emitError(err)
		return
	}

	emitFiles(out)

	/*out, err := g.Generate(targets)
	glog.V(1).Info("Processed code generator request")
	if err != nil {
		emitError(err)
		return
	}
	emitFiles(out)*/
}

func emitFiles(out []*plugin.CodeGeneratorResponse_File) {
	emitResp(&plugin.CodeGeneratorResponse{File: out})
}

func emitError(err error) {
	emitResp(&plugin.CodeGeneratorResponse{Error: proto.String(err.Error())})
}

func emitResp(resp *plugin.CodeGeneratorResponse) {
	buf, err := proto.Marshal(resp)
	if err != nil {
		glog.Fatal(err)
	}
	if _, err := os.Stdout.Write(buf); err != nil {
		glog.Fatal(err)
	}
}
