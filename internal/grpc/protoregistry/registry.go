package protoregistry

import (
	"fmt"
	"os"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/protoutil"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/reflect/protodesc"
	protoreg "google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// GitalyProtoPreregistered is a proto registry pre-registered with all
// gitalypb.GitalyProtos proto files.
var GitalyProtoPreregistered *Registry

func init() {
	exePath, err := os.Executable()
	if err != nil {
		panic(fmt.Errorf("executable path: %w", err))
	}

	if strings.HasSuffix(exePath, "gitaly-hooks") {
		GitalyProtoPreregistered, err = NewFromPaths("hook.proto")
	} else {
		GitalyProtoPreregistered, err = NewFromPaths(gitalypb.GitalyProtos...)
	}
	if err != nil {
		panic(err)
	}
}

// Registry contains info about RPC methods
type Registry struct {
	protos map[string]MethodInfo
	// interceptedMethods contains the set of methods which are intercepted
	// by Praefect instead of proxying.
	interceptedMethods map[string]struct{}
}

// New creates a new ProtoRegistry with info from one or more descriptor.FileDescriptorProto
func New(protos ...*descriptorpb.FileDescriptorProto) (*Registry, error) {
	methods := make(map[string]MethodInfo)
	interceptedMethods := make(map[string]struct{})

	for _, p := range protos {
		for _, svc := range p.GetService() {
			for _, method := range svc.GetMethod() {
				fullMethodName := formatFullMethodName(p.GetPackage(), svc.GetName(), method.GetName())

				if intercepted, err := protoutil.IsInterceptedMethod(svc, method); err != nil {
					return nil, fmt.Errorf("is intercepted: %w", err)
				} else if intercepted {
					interceptedMethods[fullMethodName] = struct{}{}
					continue
				}

				mi, err := parseMethodInfo(p, svc, method)
				if err != nil {
					return nil, err
				}

				methods[fullMethodName] = mi
			}
		}
	}

	return &Registry{
		protos:             methods,
		interceptedMethods: interceptedMethods,
	}, nil
}

// NewFromPaths returns a new Registry, initialized with the contents
// of the provided files.
func NewFromPaths(paths ...string) (*Registry, error) {
	fds := make([]*descriptorpb.FileDescriptorProto, 0, len(paths))
	for _, path := range paths {
		fd, err := protoreg.GlobalFiles.FindFileByPath(path)
		if err != nil {
			return nil, err
		}
		fds = append(fds, protodesc.ToFileDescriptorProto(fd))
	}
	return New(fds...)
}

// LookupMethod looks up an MethodInfo by service and method name
func (pr *Registry) LookupMethod(fullMethodName string) (MethodInfo, error) {
	methodInfo, ok := pr.protos[fullMethodName]
	if !ok {
		return MethodInfo{}, fmt.Errorf("full method name not found: %v", fullMethodName)
	}
	return methodInfo, nil
}

// Methods returns all registered methods
func (pr *Registry) Methods() []MethodInfo {
	methods := make([]MethodInfo, 0, len(pr.protos))
	for _, proto := range pr.protos {
		methods = append(methods, proto)
	}
	return methods
}

// IsInterceptedMethod returns whether Praefect intercepts the method call instead of proxying it.
func (pr *Registry) IsInterceptedMethod(fullMethodName string) bool {
	_, ok := pr.interceptedMethods[fullMethodName]
	return ok
}
