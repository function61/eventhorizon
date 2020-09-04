// EventHorizon system state: MQTT configuration
package ehpubsubstate

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/eventhorizon/pkg/system/ehpubsubdomain"
	"github.com/function61/gokit/syncutil"
)

type stateFormat struct {
	MqttConfig *string `json:"mqtt_config"` // serialized event
}

func newStateFormat() stateFormat {
	return stateFormat{}
}

type Store struct {
	version eh.Cursor
	mu      sync.Mutex
	state   stateFormat // for easy snapshotting
}

func New() *Store {
	return &Store{
		version: eh.SysPublishSubscribe.Beginning(),
		state:   newStateFormat(),
	}
}

func (s *Store) MqttConfig() *ehpubsubdomain.MqttConfigUpdated {
	defer lockAndUnlock(&s.mu)()

	if s.state.MqttConfig == nil {
		return nil
	}

	e, err := ehevent.Deserialize(*s.state.MqttConfig, ehpubsubdomain.Types)
	if err != nil {
		panic(err)
	}
	return e.(*ehpubsubdomain.MqttConfigUpdated)
}

func (s *Store) Version() eh.Cursor {
	defer lockAndUnlock(&s.mu)()

	return s.version
}

func (s *Store) InstallSnapshot(snap *eh.Snapshot) error {
	defer lockAndUnlock(&s.mu)()

	s.version = snap.Cursor
	s.state = stateFormat{}

	return json.Unmarshal(snap.Data, &s.state)
}

func (s *Store) Snapshot() (*eh.Snapshot, error) {
	defer lockAndUnlock(&s.mu)()

	data, err := json.MarshalIndent(s.state, "", "\t")
	if err != nil {
		return nil, err
	}

	return eh.NewSnapshot(s.version, data, s.SnapshotContextAndVersion()), nil
}

func (s *Store) SnapshotContextAndVersion() string {
	return "eh:pubsub:v1" // change if persisted stateFormat changes in backwards-incompat way
}

func (s *Store) GetEventTypes() []ehreader.LogDataKindDeserializer {
	return ehreader.EncryptedDataDeserializer(ehpubsubdomain.Types)
}

func (s *Store) ProcessEvents(_ context.Context, processAndCommit ehreader.EventProcessorHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return processAndCommit(
		s.version,
		func(ev ehevent.Event) error { return s.processEvent(ev) },
		func(version eh.Cursor) error {
			s.version = version
			return nil
		})
}

func (s *Store) processEvent(ev ehevent.Event) error {
	switch e := ev.(type) {
	case *ehpubsubdomain.MqttConfigUpdated:
		serialized := ehevent.SerializeOne(e)
		s.state.MqttConfig = &serialized
	default:
		return ehreader.UnsupportedEventTypeErr(ev)
	}

	return nil
}

type App struct {
	State  *Store
	Reader *ehreader.Reader
	Writer eh.Writer
	Logger *log.Logger
}

func LoadUntilRealtime(
	ctx context.Context,
	client *ehreader.SystemClient,
	logger *log.Logger,
) (*App, error) {
	store := New()

	a := &App{
		store,
		ehreader.NewWithSnapshots(
			store,
			client,
			logger),
		client.EventLog,
		logger}

	if err := a.Reader.LoadUntilRealtime(ctx); err != nil {
		return nil, err
	}

	return a, nil
}

var (
	lockAndUnlock = syncutil.LockAndUnlock // shorthand
)
