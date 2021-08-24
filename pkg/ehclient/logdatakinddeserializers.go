package ehclient

// Deserializers for different kinds of log data: Meta, EncryptedData etc.

import (
	"context"
	"errors"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/eheventencryption"
)

type LogDataKindDeserializer struct {
	Kind         eh.LogDataKind
	Deserializer LogDataDeserializerFn

	// otherwise could be detected from Kind==LogDataKindEncryptedData but see "synthetic statistics"
	// (which lies about deserializing LogDataKindEncryptedData but does not actually deal with encryption)
	Encryption bool
}

type LogDataDeserializerFn func(ctx context.Context, entry *eh.LogEntry, client *SystemClient) ([]ehevent.Event, error)

// returns a slice for ergonomics
func EncryptedDataDeserializer(types ehevent.Types) []LogDataKindDeserializer {
	return []LogDataKindDeserializer{
		{
			Kind:       eh.LogDataKindEncryptedData,
			Encryption: true,
			Deserializer: func(ctx context.Context, entry *eh.LogEntry, client *SystemClient) ([]ehevent.Event, error) {
				dek, err := client.LoadDEKv0(ctx, entry.Cursor.Stream())
				if err != nil {
					return nil, err
				}

				eventsSerialized, err := eheventencryption.Decrypt(entry.Data.Raw, dek)
				if err != nil {
					return nil, err
				}

				events := []ehevent.Event{}

				for _, eventSerialized := range ehevent.DeserializeLines(eventsSerialized) {
					event, err := ehevent.Deserialize(eventSerialized, types)
					if err != nil {
						var unsupp *ehevent.ErrUnsupportedEvent
						if errors.As(err, &unsupp) {
							// means it was unrecognized type (not in the map). this is necessary for forward
							// compatibility (if strict parsing is needed we can make new deserializer)
							continue
						} else {
							return nil, err
						}
					}

					events = append(events, event)
				}

				return events, nil
			},
		},
	}
}

// returns a slice for ergonomics
func MetaDeserializer() []LogDataKindDeserializer {
	return MetaDeserializer2(eh.MetaTypes)
}

func MetaDeserializer2(types ehevent.Types) []LogDataKindDeserializer {
	return []LogDataKindDeserializer{
		{
			Kind: eh.LogDataKindMeta,
			Deserializer: func(ctx context.Context, entry *eh.LogEntry, client *SystemClient) ([]ehevent.Event, error) {
				events := []ehevent.Event{}

				for _, eventSerialized := range ehevent.DeserializeLines(entry.Data.Raw) {
					metaEvent, err := ehevent.Deserialize(eventSerialized, types)
					if err != nil {
						var unsupp *ehevent.ErrUnsupportedEvent
						if errors.As(err, &unsupp) {
							// means it was unrecognized type (not in the map). this is necessary for forward
							// compatibility (if strict parsing is needed we can make new deserializer)
							continue
						} else {
							return nil, err
						}
					}

					events = append(events, metaEvent)
				}

				return events, nil
			},
		},
	}
}
