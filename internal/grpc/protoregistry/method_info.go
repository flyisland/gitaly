package protoregistry

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/protoutil"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"
	protoreg "google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// OpType represents the operation type for a RPC method
type OpType int

const (
	// OpUnknown = unknown operation type
	OpUnknown OpType = iota
	// OpAccessor = accessor operation type (ready only)
	OpAccessor
	// OpMutator = mutator operation type (modifies a repository)
	OpMutator
	// OpMaintenance is an operation which performs maintenance-tasks on the repository. It
	// shouldn't ever result in a user-visible change in behaviour, except that it may repair
	// corrupt data.
	OpMaintenance
)

// Scope represents the intended scope of an RPC method
type Scope int

const (
	// ScopeUnknown is the default scope until determined otherwise
	ScopeUnknown Scope = iota
	// ScopeRepository indicates an RPC's scope is limited to a repository
	ScopeRepository
	// ScopeStorage indicates an RPC is scoped to an entire storage location
	ScopeStorage
	// ScopePartition indicates an RPC is scoped to an entire partition
	ScopePartition
)

func (s Scope) String() string {
	switch s {
	case ScopeStorage:
		return "storage"
	case ScopeRepository:
		return "repository"
	case ScopePartition:
		return "partition"
	default:
		return fmt.Sprintf("N/A: %d", s)
	}
}

var protoScope = map[gitalypb.OperationMsg_Scope]Scope{
	gitalypb.OperationMsg_REPOSITORY: ScopeRepository,
	gitalypb.OperationMsg_STORAGE:    ScopeStorage,
	gitalypb.OperationMsg_PARTITION:  ScopePartition,
}

// MethodInfo contains metadata about the RPC method. Refer to documentation
// for message type "OperationMsg" shared.proto in ./proto for
// more documentation.
type MethodInfo struct {
	Operation   OpType
	Scope       Scope
	requestName string // protobuf message name for input type
	// requestType is the RPC's request type.
	requestType protoreflect.MessageType
	fileDesc    *descriptorpb.FileDescriptorProto
	serviceDesc *descriptorpb.ServiceDescriptorProto
	methodDesc  *descriptorpb.MethodDescriptorProto
}

// TargetRepo returns the target repository for a protobuf message if it exists
func (mi MethodInfo) TargetRepo(msg proto.Message) (*gitalypb.Repository, error) {
	return mi.getRepo(msg, gitalypb.E_TargetRepository)
}

// AdditionalRepo returns the additional repository for a Protobuf message that needs a storage
// rewritten if it exists.
func (mi MethodInfo) AdditionalRepo(msg proto.Message) (*gitalypb.Repository, error) {
	return mi.getRepo(msg, gitalypb.E_AdditionalRepository)
}

//nolint:revive // This is unintentionally missing documentation.
func (mi MethodInfo) FullMethodName() string {
	return formatFullMethodName(mi.fileDesc.GetPackage(), mi.serviceDesc.GetName(), mi.methodDesc.GetName())
}

// Package returns the name of the package the method is defined in.
func (mi MethodInfo) Package() string {
	return mi.fileDesc.GetPackage()
}

// Service returns the name of the service the method is defined in.
func (mi MethodInfo) Service() string {
	return mi.serviceDesc.GetName()
}

// Method returns the name of the method.
func (mi MethodInfo) Method() string {
	return mi.methodDesc.GetName()
}

// formatFullMethodName returns the full method name composed from the components.
func formatFullMethodName(packageName, serviceName, methodName string) string {
	return fmt.Sprintf("/%s.%s/%s", packageName, serviceName, methodName)
}

// SplitMethodName splits a full gRPC method name (e.g. "/gitaly.RepositoryService/RepositoryExists")
// into its service and method components. Returns ("unknown", "unknown") if the format is invalid.
func SplitMethodName(fullMethodName string) (service, method string) {
	fullMethodName = strings.TrimPrefix(fullMethodName, "/")
	service, method, ok := strings.Cut(fullMethodName, "/")
	if !ok {
		return "unknown", "unknown"
	}
	return service, method
}

// ErrRepositoryFieldNotFound indicates that the repository field could not be found.
var ErrRepositoryFieldNotFound = errors.New("repository field not found")

func (mi MethodInfo) getRepo(msg proto.Message, extensionType protoreflect.ExtensionType) (*gitalypb.Repository, error) {
	if mi.requestName != string(proto.MessageName(msg)) {
		return nil, fmt.Errorf(
			"proto message %s does not match expected RPC request message %s",
			proto.MessageName(msg), mi.requestName,
		)
	}

	field, err := findFieldByExtension(msg, extensionType)
	if err != nil {
		if errors.Is(err, errFieldNotFound) {
			return nil, ErrRepositoryFieldNotFound
		}

		return nil, err
	}

	if field.desc.Kind() != protoreflect.MessageKind {
		return nil, fmt.Errorf("expected repository message, got %s", field.desc.Kind().String())
	}

	switch fieldMsg := field.value.Message().Interface().(type) {
	case *gitalypb.Repository:
		return fieldMsg, nil
	case *gitalypb.ObjectPool:
		repo := fieldMsg.GetRepository()
		if repo == nil {
			return nil, ErrRepositoryFieldNotFound
		}

		return repo, nil
	default:
		return nil, fmt.Errorf("repository message has unexpected type %T", fieldMsg)
	}
}

// Storage returns the storage name for a protobuf message if it exists
func (mi MethodInfo) Storage(msg proto.Message) (string, error) {
	field, err := mi.getStorageField(msg)
	if err != nil {
		return "", err
	}

	return field.value.String(), nil
}

// SetStorage sets the storage name for a protobuf message
func (mi MethodInfo) SetStorage(msg proto.Message, storage string) error {
	field, err := mi.getStorageField(msg)
	if err != nil {
		return err
	}

	msg.ProtoReflect().Set(field.desc, protoreflect.ValueOfString(storage))

	return nil
}

func (mi MethodInfo) getStorageField(msg proto.Message) (valueField, error) {
	if mi.requestName != string(proto.MessageName(msg)) {
		return valueField{}, fmt.Errorf(
			"proto message %s does not match expected RPC request message %s",
			proto.MessageName(msg), mi.requestName,
		)
	}

	field, err := findFieldByExtension(msg, gitalypb.E_Storage)
	if err != nil {
		if errors.Is(err, errFieldNotFound) {
			return valueField{}, fmt.Errorf("target storage field not found")
		}
		return valueField{}, err
	}

	if field.desc.Kind() != protoreflect.StringKind {
		return valueField{}, fmt.Errorf("expected string, got %s", field.desc.Kind().String())
	}

	return field, nil
}

// Partition returns the partition id for a protobuf message if it exists
func (mi MethodInfo) Partition(msg proto.Message) (storage.PartitionID, error) {
	field, err := mi.getPartitionField(msg)
	if err != nil {
		return 0, err
	}

	value, err := strconv.ParseUint(field.value.String(), 10, 64)
	if err != nil {
		return 0, err
	}

	return storage.PartitionID(value), nil
}

func (mi MethodInfo) getPartitionField(msg proto.Message) (valueField, error) {
	if mi.requestName != string(proto.MessageName(msg)) {
		return valueField{}, fmt.Errorf(
			"proto message %s does not match expected RPC request message %s",
			proto.MessageName(msg), mi.requestName,
		)
	}

	field, err := findFieldByExtension(msg, gitalypb.E_PartitionId)
	if err != nil {
		if errors.Is(err, errFieldNotFound) {
			return valueField{}, fmt.Errorf("target partition field not found")
		}
		return valueField{}, err
	}

	if field.desc.Kind() != protoreflect.StringKind {
		return valueField{}, fmt.Errorf("expected string, got %s", field.desc.Kind().String())
	}

	return field, nil
}

// UnmarshalRequestProto will unmarshal the bytes into the method's request
// message type
func (mi MethodInfo) UnmarshalRequestProto(b []byte) (proto.Message, error) {
	req := mi.NewRequest()
	if err := proto.Unmarshal(b, req); err != nil {
		return nil, err
	}

	return req, nil
}

// NewRequest instantiates a new instance of the method's request type.
func (mi MethodInfo) NewRequest() proto.Message {
	return mi.requestType.New().Interface()
}

func parseMethodInfo(
	fileDesc *descriptorpb.FileDescriptorProto,
	serviceDesc *descriptorpb.ServiceDescriptorProto,
	methodDesc *descriptorpb.MethodDescriptorProto,
) (MethodInfo, error) {
	opMsg, err := protoutil.GetOpExtension(methodDesc)
	if err != nil {
		return MethodInfo{}, err
	}

	var opCode OpType

	switch opMsg.GetOp() {
	case gitalypb.OperationMsg_ACCESSOR:
		opCode = OpAccessor
	case gitalypb.OperationMsg_MUTATOR:
		opCode = OpMutator
	case gitalypb.OperationMsg_MAINTENANCE:
		opCode = OpMaintenance
	default:
		opCode = OpUnknown
	}

	// for some reason, the protobuf descriptor contains an extra dot in front
	// of the request name that the generated code does not. This trimming keeps
	// the two copies consistent for comparisons.
	requestName := strings.TrimLeft(methodDesc.GetInputType(), ".")

	requestType, err := protoreg.GlobalTypes.FindMessageByName(protoreflect.FullName(requestName))
	if err != nil {
		return MethodInfo{}, fmt.Errorf("no message type found for %w", err)
	}

	scope, ok := protoScope[opMsg.GetScopeLevel()]
	if !ok {
		return MethodInfo{}, fmt.Errorf("encountered unknown method scope %d", opMsg.GetScopeLevel())
	}

	mi := MethodInfo{
		Operation:   opCode,
		Scope:       scope,
		requestName: requestName,
		requestType: requestType,
		fileDesc:    fileDesc,
		serviceDesc: serviceDesc,
		methodDesc:  methodDesc,
	}

	return mi, nil
}

type valueField struct {
	desc  protoreflect.FieldDescriptor
	value protoreflect.Value
}

// findFieldsByExtension will search through all populated fields and returns all of those which
// have the given extension type set.
func findFieldsByExtension(msg proto.Message, extensionType protoreflect.ExtensionType) ([]valueField, error) {
	var valueFields []valueField

	if err := (protorange.Options{Stable: true}).Range(msg.ProtoReflect(), func(values protopath.Values) error {
		value := values.Index(-1)

		fieldDescriptor := value.Step.FieldDescriptor()
		if fieldDescriptor == nil {
			return nil
		}

		opts := fieldDescriptor.Options().(*descriptorpb.FieldOptions)
		if !proto.HasExtension(opts, extensionType) {
			return nil
		}

		valueFields = append(valueFields, valueField{
			desc:  fieldDescriptor,
			value: value.Value,
		})

		return nil
	}, nil); err != nil {
		return nil, fmt.Errorf("ranging over message: %w", err)
	}

	return valueFields, nil
}

var (
	errFieldNotFound  = errors.New("field not found")
	errFieldAmbiguous = errors.New("field is ambiguous")
)

// findFieldByExtension is a wrapper around findFieldsByExtension that returns a single field
// descriptor, only. Returns a errFieldNotFound error in case the field wasn't found, and a
// errFieldAmbiguous error in case there are multiple fields with the same extension.
func findFieldByExtension(msg proto.Message, extensionType protoreflect.ExtensionType) (valueField, error) {
	fields, err := findFieldsByExtension(msg, extensionType)
	if err != nil {
		return valueField{}, err
	}

	switch len(fields) {
	case 1:
		return fields[0], nil
	case 0:
		return valueField{}, errFieldNotFound
	default:
		return valueField{}, errFieldAmbiguous
	}
}
