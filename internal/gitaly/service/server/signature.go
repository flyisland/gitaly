package server

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/signature"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// ServerSignature reponds with PublicKey used by Gitaly for commit signature
func (s *server) ServerSignature(ctx context.Context, request *gitalypb.ServerSignatureRequest) (*gitalypb.ServerSignatureResponse, error) {
	if s.gitSigningKeyPath != "" {
		signingKeys, err := signature.ParseSigningKeys(s.gitSigningKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to parse signing key: %w", err)
		}

		publicKey, err := signingKeys.PublicKey()
		if err != nil {
			return nil, fmt.Errorf("failed to fetch public key: %w", err)
		}

		return &gitalypb.ServerSignatureResponse{
			PublicKey: publicKey,
		}, nil
	}

	return &gitalypb.ServerSignatureResponse{
		PublicKey: nil,
	}, nil
}
