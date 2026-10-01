// protodescriptors emits locked dependency descriptors for offline protoc generation.
package main

import (
	_ "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"os"
	"sort"
)

func main() {
	set := &descriptorpb.FileDescriptorSet{}
	protoregistry.GlobalFiles.RangeFiles(func(f protoreflect.FileDescriptor) bool {
		set.File = append(set.File, protodesc.ToFileDescriptorProto(f))
		return true
	})
	sort.Slice(set.File, func(i, j int) bool { return set.File[i].GetName() < set.File[j].GetName() })
	data, err := proto.Marshal(set)
	if err != nil {
		panic(err)
	}
	if err = os.WriteFile(os.Args[1], data, 0600); err != nil {
		panic(err)
	}
}
