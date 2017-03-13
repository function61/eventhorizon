package partitionedlossyqueue

import (
	"sync"
)

// This queue implements topic-based partitioning for possibly slow consumers
// for situations where we are only interested about the latest value of the partition.
// In this case the lower the ratio partition_count/sent_messages_across_partitions is,
// the more we benefit from discarding stuff which would increase unnecessary
// buffering or grow buffers' RAM usage unbounded.
//
// This queue guarantees that the latest message for a topic is always delivered.

type Queue struct {
	ReceiveAvailable chan bool
	mu               *sync.Mutex
	partitionStore   map[string]string
}

func New() *Queue {
	return &Queue{
		ReceiveAvailable: make(chan bool, 1),
		mu:               &sync.Mutex{},
		partitionStore:   make(map[string]string),
	}
}

func (p *Queue) ReceiveAndClear() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()

	// we can just take a reference to existing map because
	// we'll "clear" the one we store by allocating a new map for it
	ref := p.partitionStore

	// clear
	p.partitionStore = make(map[string]string)

	return ref
}

func (p *Queue) Put(partitionKey string, message string) {
	p.mu.Lock()
	// if had an entry, only the latest message survives
	p.partitionStore[partitionKey] = message
	p.mu.Unlock()

	// notify consumer in nonblocking/nonbuffering way
	select {
	case p.ReceiveAvailable <- true:
		// noop
	default:
		// notification message dropped. since the channel is buffered, we know
		// that there already was one "ReceiveAvailable" notification queued.
		// the consumer will react to it and get the latest data.
	}

}

func (p *Queue) Close() {
	// closing is ok, consumer reads any messages buffered on the channel
	close(p.ReceiveAvailable)
}
