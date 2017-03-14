package main

import (
	"errors"
	"fmt"
	ptypes "github.com/function61/pyramid/pusher/types"
)

type FailingReceiverProxy struct {
	next    ptypes.Receiver
	counter int
}

func NewFailingReceiverProxy(next ptypes.Receiver) *FailingReceiverProxy {
	return &FailingReceiverProxy{
		next:    next,
		counter: 1, // so first req fails
	}
}

func (f *FailingReceiverProxy) Push(input *ptypes.PushInput) (*ptypes.PushOutput, error) {
	if f.shouldFail() {
		return nil, errors.New(fmt.Sprintf("synthetic failure %d", f.counter))
	}

	return f.next.Push(input)
}

func (f *FailingReceiverProxy) GetSubscriptionId() (string, error) {
	return f.next.GetSubscriptionId()
	/*
		if f.shouldFail() {
			return "", errors.New("synthetic failure")
		}
	*/
}

func (f *FailingReceiverProxy) shouldFail() bool {
	defer func() { f.counter++ }()

	if f.counter%4 == 0 {
		return false // make every 4th request succeed
	}

	return true
}
