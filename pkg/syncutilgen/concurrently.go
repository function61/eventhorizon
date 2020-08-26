package syncutilgen

import (
	"context"

	"github.com/cheekybits/genny/generic"
	"golang.org/x/sync/errgroup"
)

type ItemType generic.Type

func concurrentlyItemTypeSlice(
	ctx context.Context,
	concurrency int,
	items []ItemType,
	process func(context.Context, ItemType) error,
) error {
	itemsCh := make(chan ItemType, concurrency)

	// if any of the workers error, taskCtx will be canceled.
	// taskCtx will also be canceled if parent ctx cancels.
	errGroup, taskCtx := errgroup.WithContext(ctx)

	for i := 0; i < concurrency; i++ {
		errGroup.Go(func() error {
			for item := range itemsCh {
				if err := process(taskCtx, item); err != nil {
					return err
				}
			}

			return nil
		})
	}

	for _, item := range items {
		select {
		case itemsCh <- item:
			continue
		case <-taskCtx.Done():
			// worker(s) errored -> stop submitting work. will carry on to the break
			// (break here would break only from select)
		}

		break
	}

	// makes workers exit with nil error (if no errors happened)
	close(itemsCh)

	return errGroup.Wait()
}
