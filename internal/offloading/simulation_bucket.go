package offloading

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"gocloud.dev/blob"
)

var errSimulationCanceled = errors.New("canceled")

// simulation defines the data used for intercepting or simulating method behavior.
// If Delay is set, the method will introduce a delay of the specified duration before returning.
// If Err is set, the method will return the specified error instead of the normal result.
type simulation struct {
	Delay time.Duration
	Err   error
}

// simulationBucket is a blob.Bucket with simulation setup.
type simulationBucket struct {
	// simulationMap maps an object key to a list of simulations, defining how each key should behave.
	simulationMap map[string][]simulation

	// simulationSequence is a flattened version of simulationMap.
	// It is useful for functions that need to iterate over all simulations in the map, such as list operations.
	simulationSequence     []simulation
	currentSimulationIndex int

	retryStat map[string]int
	mu        sync.Mutex
	*blob.Bucket
}

func newSimulationBucket(bucket *blob.Bucket, s map[string][]simulation) (Bucket, error) {
	seq := make([]simulation, 0)
	for _, s := range s {
		seq = append(seq, s...)
	}

	m := &simulationBucket{
		simulationMap:          s,
		Bucket:                 bucket,
		retryStat:              make(map[string]int),
		mu:                     sync.Mutex{},
		currentSimulationIndex: 0,
		simulationSequence:     seq,
	}
	return m, nil
}

func (r *simulationBucket) Download(ctx context.Context, key string, writer io.Writer, opts *blob.ReaderOptions) error {
	return r.simulate(ctx, key, func() error {
		return r.Bucket.Download(ctx, key, writer, opts)
	})
}

func (r *simulationBucket) Upload(ctx context.Context, key string, reader io.Reader, opts *blob.WriterOptions) error {
	return r.simulate(ctx, key, func() error {
		return r.Bucket.Upload(ctx, key, reader, opts)
	})
}

func (r *simulationBucket) Delete(ctx context.Context, key string) error {
	return r.simulate(ctx, key, func() error {
		return r.Bucket.Delete(ctx, key)
	})
}

// interceptedList essentially wraps the Bucket's List function.
// The difference is that it returns a listIteratorWrapper, which can inject simulation data into the blob.ListIterator.
func (r *simulationBucket) interceptedList(opts *blob.ListOptions) *listIteratorWrapper {
	defer func() {
		r.currentSimulationIndex++
	}()

	it := r.Bucket.List(opts)

	var currentSimulation *simulation
	if r.currentSimulationIndex >= len(r.simulationSequence) {
		currentSimulation = nil
	} else {
		currentSimulation = &r.simulationSequence[r.currentSimulationIndex]
	}

	return &listIteratorWrapper{
		ListIterator: it,
		simulation:   currentSimulation,
	}
}

// listIteratorWrapper wraps a blob.ListIterator and allows simulation data to be injected.
// When Next is called, it prioritizes returning the simulation data if available.
type listIteratorWrapper struct {
	simulation *simulation
	*blob.ListIterator
}

func (r *listIteratorWrapper) Next(ctx context.Context) (*blob.ListObject, error) {
	if r.simulation == nil {
		return r.ListIterator.Next(ctx)
	}

	timer := time.NewTimer(r.simulation.Delay)
	select {
	case <-ctx.Done():
		return nil, errSimulationCanceled
	case <-timer.C:
		if r.simulation.Err != nil {
			return nil, r.simulation.Err
		}
		return r.ListIterator.Next(ctx)
	}
}

func (r *simulationBucket) simulate(ctx context.Context, key string, operation func() error) error {
	r.mu.Lock()
	simulations, ok := r.simulationMap[key]
	retryCount := r.retryStat[key]

	var sim simulation
	if ok && retryCount < len(simulations) {
		sim = simulations[retryCount]
		r.retryStat[key]++
	}
	r.mu.Unlock()

	if sim.Delay > 0 {
		timer := time.NewTimer(sim.Delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errSimulationCanceled
		case <-timer.C:
			// Continue after delay
		}
	}

	if sim.Err != nil {
		return sim.Err
	}

	return operation()
}
