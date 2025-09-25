package partition

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

const invalidPartitionID = storage.PartitionID(0)

// ListPartitions returns a paginated list of all partitions.
func (s *server) ListPartitions(ctx context.Context, in *gitalypb.ListPartitionsRequest) (*gitalypb.ListPartitionsResponse, error) {
	if s.node == nil {
		return nil, structerr.NewInternal("transactions not enabled")
	}

	paginationParams := in.GetPaginationParams()
	startPartitionID := invalidPartitionID
	pageLimit := 100
	var err error
	if paginationParams != nil {
		pageLimit = int(paginationParams.GetLimit())
		startPartitionID, err = decodePageToken(paginationParams)
		if err != nil {
			return nil, structerr.NewInvalidArgument("invalid page token: %w", err)
		}
	}

	storageHandle, err := s.node.GetStorage(in.GetStorageName())
	if err != nil {
		return nil, fmt.Errorf("get storage: %w", err)
	}

	it, err := storageHandle.ListPartitions(startPartitionID)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var partitions []*gitalypb.Partition
	for it.Next() {
		partitions = append(partitions, &gitalypb.Partition{
			Id: it.GetPartitionID().String(),
		})

		if len(partitions) >= pageLimit {
			break
		}
	}

	if err := it.Err(); err != nil {
		return nil, structerr.NewInternal("list partitions: %w", err)
	}

	response := &gitalypb.ListPartitionsResponse{Partitions: partitions}

	if it.Next() {
		nextCursor, err := encodePageToken(it.GetPartitionID())
		if err != nil {
			return nil, structerr.NewInternal("creating page token: %w", err)
		}
		response.PaginationCursor = &gitalypb.PaginationCursor{NextCursor: nextCursor}

	} else if err := it.Err(); err != nil {
		return nil, structerr.NewInternal("list partitions: next page: %w", err)
	}

	return response, nil
}

func parsePartitionID(partitionIDStr string) (storage.PartitionID, error) {
	invalidPartitionID := storage.PartitionID(0)
	if partitionIDStr == "" {
		return invalidPartitionID, nil
	}

	id, err := strconv.ParseUint(partitionIDStr, 10, 64)
	if err != nil {
		return invalidPartitionID, err
	}

	return storage.PartitionID(id), nil
}

type pageToken struct {
	// PartitionID is the starting partition ID of the pagination
	PartitionID string `json:"partition_id"`
}

func decodePageToken(p *gitalypb.PaginationParameter) (storage.PartitionID, error) {
	if p.GetPageToken() != "" {
		var pageToken pageToken

		decodedString, err := base64.StdEncoding.DecodeString(p.GetPageToken())
		if err != nil {
			return invalidPartitionID, err
		}

		if err := json.Unmarshal(decodedString, &pageToken); err != nil {
			return invalidPartitionID, err
		}

		return parsePartitionID(pageToken.PartitionID)
	}

	return invalidPartitionID, nil
}

func encodePageToken(partitionID storage.PartitionID) (string, error) {
	jsonEncoded, err := json.Marshal(pageToken{PartitionID: partitionID.String()})
	if err != nil {
		return "", err
	}

	encoded := base64.StdEncoding.EncodeToString(jsonEncoded)

	return encoded, err
}
