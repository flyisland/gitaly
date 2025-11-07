package function

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Resource struct {
	Name         string    `json:"name"`
	Kind         string    `json:"kind"`
	CreationTime time.Time `json:"-"`
}

type actionFn func(ctx context.Context, resource Resource) error

// ProcessResources process each resource in resources in parallel.
// The `deleteFn` is called once per resource.
func ProcessResources(ctx context.Context, resources []Resource, deleteFn actionFn) (r Response) {
	wg := sync.WaitGroup{}

	errCh := make(chan error, 10)
	resultCh := make(chan Resource, 10)

	for _, resource := range resources {
		if resource.CreationTime.After(cutOffDate) {
			continue
		}

		wg.Add(1)
		go func(r Resource) {
			defer wg.Done()
			fmt.Printf("Processing %s %s\n", r.Kind, r.Name)
			err := deleteFn(ctx, r)
			fmt.Printf("Done processing %s %s with error: %s\n", r.Kind, r.Name, err)
			if err != nil {
				errCh <- fmt.Errorf("error deleting resource '%s' of kind %s: %v", r.Name, r.Kind, err)
				return
			}
			resultCh <- r
		}(resource)
	}

	go func() {
		defer close(errCh)
		defer close(resultCh)
		wg.Wait()
	}()

Loop:
	for {
		select {
		case err := <-errCh:
			if err != nil {
				r.Errors = append(r.Errors, err)
			}
		case res, ok := <-resultCh:
			if !ok {
				break Loop
			}
			r.DeletedResources = append(r.DeletedResources, res)
		}
	}
	return r
}
