package structerr

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

// FieldsProducer extracts metadata from err if it contains a `structerr.Error` and exposes it as
// logged fields. This function is supposed to be used with `log.MessageProducer()`.
func FieldsProducer(_ context.Context, err error) log.Fields {
	if metadata := ExtractMetadata(err); len(metadata) > 0 {
		return log.Fields{
			"error_metadata": metadata,
		}
	}

	return nil
}
