package ehclient

import (
	"github.com/function61/eventhorizon/pkg/ehevent"
)

// please have a very good reason if you use this from outside of this package
var MetaTypes = ehevent.Allocators{
	"ChildStreamCreated": func() ehevent.Event { return &ChildStreamCreated{} },
	"StreamStarted":      func() ehevent.Event { return &StreamStarted{} },
}

type ChildStreamCreated struct {
	meta   ehevent.EventMeta
	Stream string
}

func (e *ChildStreamCreated) MetaType() string         { return "ChildStreamCreated" }
func (e *ChildStreamCreated) Meta() *ehevent.EventMeta { return &e.meta }

func NewChildStreamCreated(stream string, meta ehevent.EventMeta) *ChildStreamCreated {
	return &ChildStreamCreated{meta, stream}
}

type StreamStarted struct {
	meta   ehevent.EventMeta
	Parent string
}

func (e *StreamStarted) MetaType() string         { return "StreamStarted" }
func (e *StreamStarted) Meta() *ehevent.EventMeta { return &e.meta }

func NewStreamStarted(parent string, meta ehevent.EventMeta) *StreamStarted {
	return &StreamStarted{meta, parent}
}
