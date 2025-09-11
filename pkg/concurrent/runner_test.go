package concurrent

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunner(t *testing.T) {
	require := require.New(t)
	numWorkers := 20
	r := New[int, string]().Workers(numWorkers)

	const iterations = 2048
	ints := make([]int, 0, iterations)
	for i := range iterations {
		ints = append(ints, i)
	}

	err1 := errors.New("error 1")
	err2 := errors.New("error 2")

	{
		// test error case on all workers
		res, err := r.Do(context.Background(), ints, func(i int) (string, error) {
			return "", fmt.Errorf("test error")
		})
		require.Error(err)
		require.Empty(res)
	}

	{
		// test error case on one iteration
		res, err := r.Do(context.Background(), ints, func(i int) (string, error) {
			if i == 1000 {
				return "", err1
			}
			return fmt.Sprintf("ok: %d", i), nil
		})
		require.Error(err)
		require.ErrorIs(err, err1)
		require.NotErrorIs(err, err2)
		require.Empty(res)
	}

	{
		// test error case on two iterations
		res, err := r.Do(context.Background(), ints, func(i int) (string, error) {
			if i == 1000 {
				return "", err1
			}
			if i == 2000 {
				return "", err2
			}
			return fmt.Sprintf("ok: %d", i), nil
		})
		require.Error(err)
		require.ErrorIs(err, err1)
		require.ErrorIs(err, err2)
		require.Empty(res)
	}

	{
		// no errors
		res, err := r.Do(context.Background(), ints, func(i int) (string, error) {
			return fmt.Sprintf("ok: %d", i), nil
		})
		require.NoError(err)
		require.Len(res, iterations)
		for ndx, str := range res {
			require.Equal(fmt.Sprintf("ok: %d", ndx), str)
		}
	}

	{
		// case where the number of items is fewer than the number of workers
		n := 3
		ints := make([]int, 0, n)
		for i := range n {
			ints = append(ints, i)
		}
		res, err := r.Do(context.Background(), ints, func(i int) (string, error) {
			return fmt.Sprintf("ok: %d", i), nil
		})
		require.NoError(err)
		require.Len(res, 3)
		require.Equal("ok: 2", res[2])
	}

	{
		// check to ensure that canceling the context part way through iteration
		// stops loop iteration
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		count := atomic.Int32{}

		res, err := r.Do(ctx, ints, func(i int) (string, error) {
			if i == 1000 {
				cancel()
				return "", nil
			}

			count.Add(1)
			return "", nil
		})
		require.Error(err)
		require.ErrorIs(err, context.Canceled)
		require.Empty(res)

		num := count.Load()
		require.NotZero(num)
		require.Less(num, int32(iterations))
	}
}
