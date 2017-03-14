package main

import (
	ptypes "github.com/function61/pyramid/pusher/types"
	rtypes "github.com/function61/pyramid/reader/types"
)

type FailingReceiverProxy struct {
	next ptypes.Receiver
}

func NewFailingReceiverProxy(next ptypes.Receiver) *FailingReceiverProxy {
	return &FailingReceiverProxy{
		next: next,
	}
}

func (f *FailingReceiverProxy) PushReadResult(result *rtypes.ReadResult) (*ptypes.PushResult, error) {
	return f.next.PushReadResult(result)
}

func (f *FailingReceiverProxy) GetSubscriptionId() (string, error) {
	return f.next.GetSubscriptionId()
}
