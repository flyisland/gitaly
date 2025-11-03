package function

import (
	"context"
	"fmt"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"google.golang.org/api/iterator"
)

const (
	InstanceKind string = "instance"
)

// Instances deletes all instances that matches `labelFilter` and that
// are older than `cutOffDate`.
func Instances(ctx context.Context) Response {
	response := NewResponse()

	instancesClient, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		response.Errors = append(response.Errors, fmt.Errorf("cannot delete instances; failed to create instances client: %w", err))
		return response
	}

	defer func() { _ = instancesClient.Close() }()

	instanceList := instancesClient.List(ctx, &computepb.ListInstancesRequest{
		Filter:  &labelFilter,
		Project: GcpProjectID,
		Zone:    GcpZone,
	})

	resources := make([]Resource, 0)
	for {
		instance, err := instanceList.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			response.Errors = append(response.Errors, fmt.Errorf("error fetching next instance from iterator: %w", err))
			continue
		}

		creationTime, err := time.Parse(time.RFC3339, instance.GetCreationTimestamp())
		if err != nil {
			response.Errors = append(response.Errors, fmt.Errorf("error parsing instance '%s' creation timestamp: %w", instance.GetName(), err))
			continue
		}
		resources = append(resources, Resource{
			Name:         instance.GetName(),
			Kind:         InstanceKind,
			CreationTime: creationTime,
		})
	}

	deleteFn := func(ctx context.Context, r Resource) error {
		deleteReq := computepb.DeleteInstanceRequest{
			Instance: r.Name,
			Project:  GcpProjectID,
			Zone:     GcpZone,
		}
		op, err := instancesClient.Delete(ctx, &deleteReq)
		if err != nil {
			return fmt.Errorf("error deleting instance %q: %v", r.Name, err)
		}
		if err := op.Wait(ctx); err != nil {
			return fmt.Errorf("error waiting for instance %s deletion: %v", r.Name, err)
		}
		return nil
	}

	processResponse := ProcessResources(ctx, resources, deleteFn)
	response.DeletedResources = processResponse.DeletedResources
	response.Errors = append(response.Errors, processResponse.Errors...)
	return response
}
