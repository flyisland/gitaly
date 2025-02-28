package storage

import (
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protodelim"
)

// RepositoryKeyPrefix is the prefix used for storing keys recording repository
// existence in a partition.
const RepositoryKeyPrefix = "r/"

// kvStateFileName is the filename the kv state is written to
var kvStateFileName = "kv-state"

// RepositoryKey generates the database key for recording repository existence in a partition.
func RepositoryKey(relativePath string) []byte {
	return []byte(RepositoryKeyPrefix + relativePath)
}

// CreateKvFile creates a file storing the transaction snapshot's kv state
func CreateKvFile(tx Transaction) (kvFile *os.File, returnErr error) {
	kvFile, err := os.CreateTemp("", kvStateFileName)
	if err != nil {
		return nil, fmt.Errorf("create temp file for KV entries: %w", err)
	}

	if err := os.Remove(kvFile.Name()); err != nil {
		return nil, fmt.Errorf("remove temp KV file: %w", err)
	}

	kvIter := tx.KV().NewIterator(keyvalue.IteratorOptions{})
	defer kvIter.Close()
	for kvIter.Rewind(); kvIter.Valid(); kvIter.Next() {
		item := kvIter.Item()

		if err := item.Value(func(v []byte) error {
			if _, err := protodelim.MarshalTo(kvFile, &gitalypb.KVPair{Key: item.Key(), Value: v}); err != nil {
				return fmt.Errorf("write KV entry to temp file: %w", err)
			}

			return nil
		}); err != nil {
			return nil, fmt.Errorf("get KV value: %w", err)
		}
	}

	// Rewind the temp file to the beginning before reading from it.
	if _, err := kvFile.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("rewind KV entries file: %w", err)
	}
	return kvFile, nil
}
