package server

import (
	"bytes"
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// ServerSignature obtains the public key from all storage nodes and
// ensures they are the same before returning it to the client.
func (s *Server) ServerSignature(ctx context.Context, request *gitalypb.ServerSignatureRequest) (*gitalypb.ServerSignatureResponse, error) {
	var serverSignature *gitalypb.ServerSignatureResponse

	for _, storages := range s.conns {
		for storage, conn := range storages {
			client := gitalypb.NewServerServiceClient(conn)
			resp, err := client.ServerSignature(ctx, &gitalypb.ServerSignatureRequest{})
			if err != nil {
				return nil, fmt.Errorf("request signature from Praefect storage %q: %w", storage, err)
			}

			if serverSignature == nil {
				serverSignature = resp
			} else if !bytes.Equal(serverSignature.GetPublicKey(), resp.GetPublicKey()) {
				return nil, fmt.Errorf("signature mismatch with Praefect storage %q", storage)
			}
		}
	}

	return serverSignature, nil
}
