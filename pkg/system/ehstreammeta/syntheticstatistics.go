package ehstreammeta

import (
	"context"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
)

// lies that we want to deserialize everything, even encrypted data. but we
// only take note of the statistics (byte size of their data) as a synthetic event.
//
// meta events we actually deserialize, but also inject synthetic statistics.
func injectSyntheticStatisticsDeserializer(metaDeserializer ehclient.LogDataKindDeserializer) []ehclient.LogDataKindDeserializer {
	return []ehclient.LogDataKindDeserializer{
		{
			Kind:       eh.LogDataKindEncryptedData,
			Encryption: false, // zero value would've sufficed, but leaving this here for documentation reasons
			Deserializer: func(ctx context.Context, entry *eh.LogEntry, client *ehclient.SystemClient) ([]ehevent.Event, error) {
				return []ehevent.Event{newSyntheticStatisticsEvent(len(entry.Data.Raw))}, nil
			},
		},
		{
			Kind: eh.LogDataKindMeta,
			Deserializer: func(ctx context.Context, entry *eh.LogEntry, client *ehclient.SystemClient) ([]ehevent.Event, error) {
				events, err := metaDeserializer.Deserializer(ctx, entry, client)
				if err != nil {
					return nil, err
				}

				// augment with computed
				events = append(events, newSyntheticStatisticsEvent(len(entry.Data.Raw)))

				return events, nil
			},
		},
	}
}

func newSyntheticStatisticsEvent(numBytes int) *syntheticStatisticsEvent {
	return &syntheticStatisticsEvent{
		NumBytes: numBytes,
	}
}

// captures number of bytes written to the stream in a single *LogEntry*.
// the count of these *syntheticStatisticsEvent* is the count of *LogEntry*s encountered.
type syntheticStatisticsEvent struct {
	meta     ehevent.EventMeta
	NumBytes int
}

func (e *syntheticStatisticsEvent) MetaType() string         { return "syntheticStatisticsEvent" }
func (e *syntheticStatisticsEvent) Meta() *ehevent.EventMeta { return &e.meta }
