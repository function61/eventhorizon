// Structure of data for all state changes
package ehsubscriptiondomain

import (
	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
)

var Types = ehevent.Types{
	"$subscription.Activity": func() ehevent.Event { return &SubscriptionActivity{} },
}

// ------

type SubscriptionActivity struct {
	meta  ehevent.EventMeta
	Heads []eh.CursorCompact
}

func (e *SubscriptionActivity) MetaType() string         { return "$subscription.Activity" }
func (e *SubscriptionActivity) Meta() *ehevent.EventMeta { return &e.meta }

func NewSubscriptionActivity(
	heads []eh.Cursor,
	meta ehevent.EventMeta,
) *SubscriptionActivity {
	headsCompact := []eh.CursorCompact{}
	for _, head := range heads {
		headsCompact = append(headsCompact, eh.NewCursorCompact(head))
	}

	return &SubscriptionActivity{
		meta:  meta,
		Heads: headsCompact,
	}
}
