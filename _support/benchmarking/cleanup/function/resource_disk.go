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
	DiskKind string = "disk"
)

// Disks deletes all disks that matches `labelFilter` and that
// are older than `cutOffDate`.
func Disks(ctx context.Context) Response {
	response := NewResponse()

	disksClient, err := compute.NewDisksRESTClient(ctx)
	if err != nil {
		response.Errors = append(response.Errors, fmt.Errorf("cannot delete disks; failed to create disks client: %w", err))
		return response
	}

	defer func() { _ = disksClient.Close() }()

	diskList := disksClient.List(ctx, &computepb.ListDisksRequest{
		Filter:  &labelFilter,
		Project: GcpProjectID,
		Zone:    GcpZone,
	})

	resources := make([]Resource, 0)
	for {
		disk, err := diskList.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			response.Errors = append(response.Errors, fmt.Errorf("error fetching next disk from iterator: %w", err))
			continue
		}

		creationTime, err := time.Parse(time.RFC3339, disk.GetCreationTimestamp())
		if err != nil {
			response.Errors = append(response.Errors, fmt.Errorf("error parsing disk '%s' creation timestamp: %w", disk.GetName(), err))
			continue
		}
		resources = append(resources, Resource{
			Name:         disk.GetName(),
			Kind:         DiskKind,
			CreationTime: creationTime,
		})
	}

	deleteFn := func(ctx context.Context, r Resource) error {
		deleteReq := computepb.DeleteDiskRequest{
			Disk:    r.Name,
			Project: GcpProjectID,
			Zone:    GcpZone,
		}
		op, err := disksClient.Delete(ctx, &deleteReq)
		if err != nil {
			return fmt.Errorf("error deleting disk %q: %v", r.Name, err)
		}
		if err := op.Wait(ctx); err != nil {
			return fmt.Errorf("error waiting for disk %s deletion: %v", r.Name, err)
		}
		return nil
	}

	processResponse := ProcessResources(ctx, resources, deleteFn)
	response.DeletedResources = processResponse.DeletedResources
	response.Errors = append(response.Errors, processResponse.Errors...)
	return response
}
