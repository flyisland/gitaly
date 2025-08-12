package storage

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
)

// TransactionID is an ID that uniquely identifies a Transaction.
type TransactionID uint64

// keyTransactionID is the context key storing a TransactionID.
type keyTransactionID struct{}

// ContextWithTransactionID stores the transaction ID in the context.
func ContextWithTransactionID(ctx context.Context, id TransactionID) context.Context {
	return context.WithValue(ctx, keyTransactionID{}, id)
}

// ExtractTransactionID extracts the transaction ID from the context. The returned ID is zero
// if there was no transaction ID in the context.
func ExtractTransactionID(ctx context.Context) TransactionID {
	value := ctx.Value(keyTransactionID{})
	if value == nil {
		return 0
	}

	return value.(TransactionID)
}

type keyTransaction struct{}

// ContextWithTransaction stores the transaction into the context.
func ContextWithTransaction(ctx context.Context, tx Transaction) context.Context {
	return context.WithValue(ctx, keyTransaction{}, tx)
}

// ExtractTransaction extracts the transaction from the context. Nil is returned if there's
// no transaction in the context.
func ExtractTransaction(ctx context.Context) Transaction {
	value := ctx.Value(keyTransaction{})
	if value == nil {
		return nil
	}

	return value.(Transaction)
}

type keyPartitionInfo struct{}

type partitionInfo struct {
	PartitionKey *gitalypb.RaftPartitionKey
	MemberID     uint64
	RelativePath string
}

// ContextWithPartitionInfo stores the partition info into the context.
// This is used to pass the original partition info to the partition factory.
func ContextWithPartitionInfo(ctx context.Context, partitionKey *gitalypb.RaftPartitionKey, memberID uint64, relativePath string) context.Context {
	return context.WithValue(ctx, keyPartitionInfo{}, partitionInfo{
		PartitionKey: partitionKey,
		MemberID:     memberID,
		RelativePath: relativePath,
	})
}

// ExtractPartitionInfo extracts the partition info from the context. Nil is returned if there's
// no partition info in the context.
func ExtractPartitionInfo(ctx context.Context) partitionInfo {
	value := ctx.Value(keyPartitionInfo{})
	if value == nil {
		return partitionInfo{}
	}

	return value.(partitionInfo)
}

const keyPartitioningHint = "gitaly-partitioning-hint"

// ContextWithPartitioningHint stores the relativePath as a partitioning hint into the incoming
// gRPC metadata in the context.
func ContextWithPartitioningHint(ctx context.Context, relativePath string) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	} else {
		md = md.Copy()
	}
	md.Set(keyPartitioningHint, relativePath)

	return metadata.NewIncomingContext(ctx, md)
}

// ExtractPartitioningHint extracts the partitioning hint from the incoming gRPC
// metadata in the context. Empty string is returned if no partitioning hint was provided.
// An error is returned if the metadata in the context contained multiple partitioning hints.
func ExtractPartitioningHint(ctx context.Context) (string, error) {
	relativePaths := metadata.ValueFromIncomingContext(ctx, keyPartitioningHint)
	if len(relativePaths) > 1 {
		return "", errors.New("multiple partitioning hints")
	}

	if len(relativePaths) == 0 {
		// No partitioning hint was set.
		return "", nil
	}

	return relativePaths[0], nil
}

// ContextWithoutPartitioningHint removes the partitioning hint from the provided context.
func ContextWithoutPartitioningHint(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	} else {
		md = md.Copy()
	}
	md.Delete(keyPartitioningHint)

	return metadata.NewIncomingContext(ctx, md)
}
