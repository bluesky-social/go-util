package concurrent

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/semaphore"
)

// Runner executes a given function cross N goroutines in parallel and accumulates the results
type Runner[T, V any] struct {
	workers int64
}

// Creates a Runner that is ready for use
func New[T, V any]() *Runner[T, V] {
	return &Runner[T, V]{
		workers: 50,
	}
}

// Sets the max number of concurrent goroutines to process work
func (r *Runner[T, V]) Workers(workers int) *Runner[T, V] {
	r.workers = int64(workers)
	return r
}

// Executes `f` across the specified number of goroutines and accumulates the result
func (r *Runner[T, V]) Do(ctx context.Context, items []T, f func(T) (V, error)) ([]V, error) {
	results := make([]V, len(items))
	errs := make([]error, len(items))

	sema := semaphore.NewWeighted(r.workers)
	for ndx, item := range items {
		if err := sema.Acquire(ctx, 1); err != nil {
			return nil, fmt.Errorf("unable to acquire semaphore: %w", err)
		}

		go func(i int, it T) {
			defer sema.Release(1)

			res, err := f(it)
			if err != nil {
				errs[i] = err
				return
			}

			results[i] = res
		}(ndx, item)
	}
	if err := sema.Acquire(ctx, r.workers); err != nil {
		return nil, fmt.Errorf("unable to acquire semaphore: %w", err)
	}

	err := errors.Join(errs...)
	if err != nil {
		return nil, err
	}

	return results, nil
}
