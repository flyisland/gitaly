package storage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/env"
	"google.golang.org/grpc/metadata"
)

// ServerInfo contains information about how to reach a Gitaly server or a
// Praefect virtual storage. This is necessary for Gitaly RPC's involving more
// than one Gitaly. Without this information, Gitaly would not know how to reach
// the remote peer.
type ServerInfo struct {
	Address string `json:"address"`
	Token   string `json:"token"`
}

// Zero returns true when no attributes have been set.
func (si ServerInfo) Zero() bool {
	return si == ServerInfo{}
}

// GitalyServers hold Gitaly servers info like {"default":{"token":"x","address":"y"}},
// to be passed in `gitaly-servers` metadata.
type GitalyServers map[string]ServerInfo

// ErrEmptyMetadata indicates that the gRPC metadata was not found in the
// context
var ErrEmptyMetadata = errors.New("empty metadata")

// ExtractGitalyServers extracts `storage.GitalyServers` from an incoming context.
func ExtractGitalyServers(ctx context.Context) (gitalyServersInfo GitalyServers, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, ErrEmptyMetadata
	}

	gitalyServersJSONEncoded := md["gitaly-servers"]
	if len(gitalyServersJSONEncoded) == 0 {
		return nil, fmt.Errorf("empty gitaly-servers metadata")
	}

	if err := unmarshalGitalyServers(gitalyServersJSONEncoded[0], &gitalyServersInfo); err != nil {
		return nil, err
	}
	return gitalyServersInfo, err
}

// ExtractGitalyServer extracts server information for a specific storage
func ExtractGitalyServer(ctx context.Context, storageName string) (ServerInfo, error) {
	gitalyServers, err := ExtractGitalyServers(ctx)
	if err != nil {
		return ServerInfo{}, err
	}

	gitalyServer, ok := gitalyServers[storageName]
	if !ok {
		return ServerInfo{}, errors.New("storage name not found")
	}

	return gitalyServer, nil
}

// InjectGitalyServers injects gitaly-servers metadata into an outgoing context
func InjectGitalyServers(ctx context.Context, name, address, token string) (context.Context, error) {
	gitalyServers := GitalyServers{
		name: {
			Address: address,
			Token:   token,
		},
	}

	gitalyServersJSON, err := json.Marshal(gitalyServers)
	if err != nil {
		return nil, err
	}

	return metadata.AppendToOutgoingContext(ctx, "gitaly-servers", base64.StdEncoding.EncodeToString(gitalyServersJSON)), nil
}

// InjectGitalyServersEnv injects the GITALY_SERVERS env var into an incoming
// context.
func InjectGitalyServersEnv(ctx context.Context) (context.Context, error) {
	rawServers := env.GetString("GITALY_SERVERS", "")
	if rawServers == "" {
		return ctx, nil
	}

	// Make sure we fail early if the value in the env var cannot be interpreted.
	if err := unmarshalGitalyServers(rawServers, &GitalyServers{}); err != nil {
		return nil, fmt.Errorf("injecting GITALY_SERVERS: %w", err)
	}

	md := metadata.Pairs("gitaly-servers", rawServers)
	return metadata.NewIncomingContext(ctx, md), nil
}

func unmarshalGitalyServers(encoded string, servers *GitalyServers) error {
	gitalyServersJSON, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("failed decoding base64: %w", err)
	}

	if err := json.Unmarshal(gitalyServersJSON, servers); err != nil {
		return fmt.Errorf("failed unmarshalling json: %w", err)
	}

	return nil
}
