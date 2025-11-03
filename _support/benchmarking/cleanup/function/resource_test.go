package function

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProcessResources(t *testing.T) {
	readyForDeletion := time.Now().Add(time.Hour * 24 * -8)
	notReadyForDeletion := time.Now().Add(time.Hour * 24 * -6)

	tests := []struct {
		name      string
		resources []Resource
		actionFn  func(context.Context, Resource) error
		want      Response
	}{
		{
			name: "When all resources are ready for deletion, they should be deleted",
			resources: []Resource{
				{
					Name:         "instance1",
					Kind:         "instance",
					CreationTime: readyForDeletion,
				},
				{
					Name:         "instance2",
					Kind:         "instance",
					CreationTime: readyForDeletion,
				},
				{
					Name:         "instance3",
					Kind:         "instance",
					CreationTime: readyForDeletion,
				},
			},
			actionFn: func(_ context.Context, _ Resource) error { return nil },
			want: Response{
				DeletedResources: []Resource{
					{
						Name:         "instance1",
						Kind:         "instance",
						CreationTime: readyForDeletion,
					},
					{
						Name:         "instance2",
						Kind:         "instance",
						CreationTime: readyForDeletion,
					},
					{
						Name:         "instance3",
						Kind:         "instance",
						CreationTime: readyForDeletion,
					},
				},
				Errors: []error{},
			},
		},
		{
			name: "Should only deleted resources ready for deletion",
			resources: []Resource{
				{
					Name:         "instance1",
					Kind:         "instance",
					CreationTime: readyForDeletion,
				},
				{
					Name:         "instance2",
					Kind:         "instance",
					CreationTime: notReadyForDeletion,
				},
			},
			actionFn: func(_ context.Context, _ Resource) error { return nil },
			want: Response{
				DeletedResources: []Resource{
					{
						Name:         "instance1",
						Kind:         "instance",
						CreationTime: readyForDeletion,
					},
				},
				Errors: []error{},
			},
		},
		{
			name: "Should return errors",
			resources: []Resource{
				{
					Name:         "instance1",
					Kind:         "instance",
					CreationTime: readyForDeletion,
				},
				{
					Name:         "instance2",
					Kind:         "instance",
					CreationTime: notReadyForDeletion,
				},
				{
					Name:         "instance3",
					Kind:         "instance",
					CreationTime: readyForDeletion,
				},
			},
			actionFn: func(_ context.Context, r Resource) error {
				if r.Name == "instance3" {
					return errors.New("some error")
				}
				return nil
			},
			want: Response{
				DeletedResources: []Resource{
					{
						Name:         "instance1",
						Kind:         "instance",
						CreationTime: readyForDeletion,
					},
				},
				Errors: []error{
					errors.New("error deleting resource 'instance3' of kind instance: some error"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			response := ProcessResources(ctx, tt.resources, tt.actionFn)

			require.ElementsMatch(t, tt.want.DeletedResources, response.DeletedResources)
			require.ElementsMatch(t, tt.want.Errors, response.Errors)
		})
	}
}
