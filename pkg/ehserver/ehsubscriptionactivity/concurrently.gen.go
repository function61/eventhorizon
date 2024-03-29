// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

//nolint:unused,deadcode
// Code is generated based off of this file, so "unused code" is false positive
package ehsubscriptionactivity

import (
	"context"

	"github.com/function61/eventhorizon/pkg/eh"
	"golang.org/x/sync/errgroup"
)

func concurrentlyEhCursorSlice(
	ctx context.Context,
	concurrency int,
	items []eh.Cursor,
	process func(context.Context, eh.Cursor) error,
) error {
	itemsCh := make(chan eh.Cursor, concurrency)

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

//nolint:unused,deadcode
// Code is generated based off of this file, so "unused code" is false positive

func concurrentlySubscriptionActivitySlice(
	ctx context.Context,
	concurrency int,
	items []*subscriptionActivity,
	process func(context.Context, *subscriptionActivity) error,
) error {
	itemsCh := make(chan *subscriptionActivity, concurrency)

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
