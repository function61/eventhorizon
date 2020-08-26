package ehsubscriptionactivity

import (
	"sync"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/gokit/syncutil"
)

// overall maximum cursors (not yet partitioned by subscription)
type DiscoveredMaxCursors struct {
	streams map[string]eh.Cursor
}

func NewDiscoveredMaxCursors() *DiscoveredMaxCursors {
	return &DiscoveredMaxCursors{
		streams: map[string]eh.Cursor{},
	}
}

// filter out subscriptions, because if we'd post subscription activity for them,
// it would yield an infinite loop
func (d *DiscoveredMaxCursors) StreamsWithoutSubscriptionStreams() []eh.Cursor {
	filtered := []eh.Cursor{}
	for _, stream := range d.streams {
		if !stream.Stream().IsUnder(eh.SysSubscriptions) {
			filtered = append(filtered, stream)
		}
	}

	return filtered
}

func (d *DiscoveredMaxCursors) Add(cursor eh.Cursor) {
	existing, had := d.streams[cursor.Stream().String()]
	if !had {
		d.streams[cursor.Stream().String()] = cursor
		existing = cursor
	}

	if existing.Before(cursor) {
		d.streams[cursor.Stream().String()] = cursor
	}
}

type allSubscriptionsActivities struct {
	sub   map[string]*subscriptionActivity
	subMu sync.Mutex
}

func newAllSubscriptionsActivities() *allSubscriptionsActivities {
	return &allSubscriptionsActivities{
		sub: map[string]*subscriptionActivity{},
	}
}

func (a *allSubscriptionsActivities) Subscriptions() []*subscriptionActivity {
	subs := []*subscriptionActivity{}
	for _, item := range a.sub {
		subs = append(subs, item)
	}

	return subs
}

func (a *allSubscriptionsActivities) UpsertSubscription(sub eh.SubscriptionId) *subscriptionActivity {
	defer syncutil.LockAndUnlock(&a.subMu)()

	key := sub.String()

	sact, got := a.sub[key]
	if !got {
		a.sub[key] = newSubscriptionActivity(sub)
		sact = a.sub[key]
	}
	return sact
}

type subscriptionActivity struct {
	subscription eh.SubscriptionId
	cursors      []eh.Cursor
	cursorsMu    sync.Mutex
}

func newSubscriptionActivity(subscription eh.SubscriptionId) *subscriptionActivity {
	return &subscriptionActivity{
		subscription: subscription,
		cursors:      []eh.Cursor{},
	}
}

func (s *subscriptionActivity) Append(cursor eh.Cursor) {
	defer syncutil.LockAndUnlock(&s.cursorsMu)()

	s.cursors = append(s.cursors, cursor)
}
