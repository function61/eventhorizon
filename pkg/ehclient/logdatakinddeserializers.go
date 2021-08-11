package ehclient

// Deserializers for different kinds of log data: Meta, EncryptedData etc.

import (
	"context"

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
				events := []ehevent.Event{}

				dek, err := client.LoadDek(ctx, entry.Cursor.Stream())
				if err != nil {
					return nil, err
				}

				eventsSerialized, err := eheventencryption.Decrypt(entry.Data, dek)
				if err != nil {
					return nil, err
				}

				for _, eventSerialized := range eventsSerialized {
					event, err := ehevent.Deserialize(eventSerialized, types)
					if err != nil {
						return nil, err
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
	return []LogDataKindDeserializer{
		{
			Kind: eh.LogDataKindMeta,
			Deserializer: func(ctx context.Context, entry *eh.LogEntry, client *SystemClient) ([]ehevent.Event, error) {
				metaEvent, err := ehevent.Deserialize(string(entry.Data.Raw), eh.MetaTypes)
				if err != nil {
					return nil, err
				}

				return []ehevent.Event{metaEvent}, nil
			},
		},
	}
}
