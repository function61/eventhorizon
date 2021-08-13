package eh

import (
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/gokit/crypto/envelopeenc"
)

func LogDataMeta(e ehevent.Event) *LogData {
	return &LogData{
		Kind: LogDataKindMeta,
		Raw:  []byte(ehevent.SerializeOne(e)),
	}
}

// - these are common events that can appear in *any* application-stream. contrast this with
//   system streams like "/$/credentials" where events are application-specific streams
//   (b/c they can't appear anywhere else)
// - please have a very good reason if you use this from outside of this package.
var MetaTypes = ehevent.Types{
	"$stream.ChildStreamCreated": func() ehevent.Event { return &StreamChildStreamCreated{} },
	"$stream.Started":            func() ehevent.Event { return &StreamStarted{} },
	"$subscription.Subscribed":   func() ehevent.Event { return &SubscriptionSubscribed{} },
	"$subscription.Unsubscribed": func() ehevent.Event { return &SubscriptionUnsubscribed{} },
}

// ------

type StreamChildStreamCreated struct {
	meta   ehevent.EventMeta
	Stream StreamName
}

func (e *StreamChildStreamCreated) MetaType() string         { return "$stream.ChildStreamCreated" }
func (e *StreamChildStreamCreated) Meta() *ehevent.EventMeta { return &e.meta }

func NewStreamChildStreamCreated(stream StreamName, meta ehevent.EventMeta) *StreamChildStreamCreated {
	return &StreamChildStreamCreated{meta, stream}
}

// ------

type StreamStarted struct {
	meta       ehevent.EventMeta
	DEK        envelopeenc.Envelope // Data Encryption Key (DEK) envelope (see pkg envelopeenc)
	KeyGroupId string
}

func (e *StreamStarted) MetaType() string         { return "$stream.Started" }
func (e *StreamStarted) Meta() *ehevent.EventMeta { return &e.meta }

func NewStreamStarted(dek envelopeenc.Envelope, keyGroupId string, meta ehevent.EventMeta) *StreamStarted {
	return &StreamStarted{meta, dek, keyGroupId}
}

// ------

type SubscriptionSubscribed struct {
	meta ehevent.EventMeta
	ID   SubscriberID
}

func (e *SubscriptionSubscribed) MetaType() string         { return "$subscription.Subscribed" }
func (e *SubscriptionSubscribed) Meta() *ehevent.EventMeta { return &e.meta }

func NewSubscriptionSubscribed(id SubscriberID, meta ehevent.EventMeta) *SubscriptionSubscribed {
	return &SubscriptionSubscribed{meta, id}
}

// ------

type SubscriptionUnsubscribed struct {
	meta ehevent.EventMeta
	ID   SubscriberID
}

func (e *SubscriptionUnsubscribed) MetaType() string         { return "$subscription.Unsubscribed" }
func (e *SubscriptionUnsubscribed) Meta() *ehevent.EventMeta { return &e.meta }

func NewSubscriptionUnsubscribed(id SubscriberID, meta ehevent.EventMeta) *SubscriptionUnsubscribed {
	return &SubscriptionUnsubscribed{meta, id}
}
