package featureflag

import (
	"context"
	"sort"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

// FieldsProducer adds feature_flags logging fields to gRPC logs. Only enabled flags are available.
// The log field looks something like following.
//
//	{
//	   "feature_flags": "feature_a feature_b feature_c"
//	}
func FieldsProducer(ctx context.Context, err error) log.Fields {
	var enabledFlags []string
	for flag, value := range featureflag.FromContext(ctx) {
		if value {
			enabledFlags = append(enabledFlags, flag.Name)
		}
	}
	if len(enabledFlags) == 0 {
		return nil
	}

	sort.Strings(enabledFlags)
	return log.Fields{
		"feature_flags": strings.Join(enabledFlags, " "),
	}
}
