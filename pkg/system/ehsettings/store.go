// EventHorizon system state: KEKs, key groups, keyservers, MQTT configuration etc.
package ehsettings

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/system/ehsettingsdomain"
	"github.com/function61/gokit/sliceutil"
	"github.com/function61/gokit/sync/syncutil"
)

type stateFormat struct {
	MqttConfig *string `json:"mqtt_config"` // serialized event
	KeyServers []*KeyServer
	Keks       []*Kek
}

type KeyServer struct {
	Id             string
	Created        time.Time
	Label          string
	AttachedKekIds []string
}

type Kek struct {
	Id         string
	Registered time.Time
	Kind       string
	Label      string
	PublicKey  string
}

func newStateFormat() stateFormat {
	return stateFormat{
		KeyServers: []*KeyServer{},
		Keks:       []*Kek{},
	}
}

type Store struct {
	version eh.Cursor
	mu      sync.Mutex
	state   stateFormat // for easy snapshotting
}

func New() *Store {
	return &Store{
		version: eh.SysSettings.Beginning(),
		state:   newStateFormat(),
	}
}

func (s *Store) MqttConfig() *ehsettingsdomain.MqttConfigUpdated {
	defer lockAndUnlock(&s.mu)()

	if s.state.MqttConfig == nil {
		return nil
	}

	e, err := ehevent.Deserialize(*s.state.MqttConfig, ehsettingsdomain.Types)
	if err != nil {
		panic(err)
	}

	return e.(*ehsettingsdomain.MqttConfigUpdated)
}

func (s *Store) Kek(kekId string) *Kek {
	defer lockAndUnlock(&s.mu)()

	for _, kek := range s.state.Keks {
		if kek.Id == kekId {
			return kek
		}
	}

	return nil
}

func (s *Store) KeyServerWithKekAttached(kekId string) *KeyServer {
	defer lockAndUnlock(&s.mu)()

	for _, keyServer := range s.state.KeyServers {
		if sliceutil.ContainsString(keyServer.AttachedKekIds, kekId) {
			found := *keyServer // make a copy (TODO: this is not a deep copy)
			return &found
		}
	}

	return nil
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
	return "eh:settings:v1" // change if persisted stateFormat changes in backwards-incompat way
}

func (s *Store) GetEventTypes() []ehclient.LogDataKindDeserializer {
	return ehclient.EncryptedDataDeserializer(ehsettingsdomain.Types)
}

func (s *Store) ProcessEvents(_ context.Context, processAndCommit ehclient.EventProcessorHandler) error {
	defer lockAndUnlock(&s.mu)()

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
	case *ehsettingsdomain.MqttConfigUpdated:
		serialized := ehevent.SerializeOne(e)
		s.state.MqttConfig = &serialized
	case *ehsettingsdomain.KekRegistered:
		s.state.Keks = append(s.state.Keks, &Kek{
			Id:         e.Id,
			Registered: e.Meta().Time(),
			Kind:       e.Kind,
			Label:      e.Label,
			PublicKey:  e.PublicKey,
		})
	case *ehsettingsdomain.KeyserverCreated:
		s.state.KeyServers = append(s.state.KeyServers, &KeyServer{
			Id:             e.Id,
			Created:        e.Meta().Time(),
			Label:          e.Label,
			AttachedKekIds: []string{},
		})
	case *ehsettingsdomain.KeyserverKeyAttached:
		ks, err := s.keyServerById(e.Server)
		if err != nil {
			return err
		}

		ks.AttachedKekIds = sliceutil.FilterString(ks.AttachedKekIds, func(item string) bool { return item != e.Key })

		ks.AttachedKekIds = append(ks.AttachedKekIds, e.Key)
	case *ehsettingsdomain.KeyserverKeyDetached:
		ks, err := s.keyServerById(e.Server)
		if err != nil {
			return err
		}

		ks.AttachedKekIds = sliceutil.FilterString(ks.AttachedKekIds, func(item string) bool { return item != e.Key })
	case *ehsettingsdomain.KeygroupCreated:
		// TODO: ignore
	default:
		return ehclient.UnsupportedEventTypeErr(ev)
	}

	return nil
}

func (s *Store) keyServerById(id string) (*KeyServer, error) {
	for _, ks := range s.state.KeyServers {
		if ks.Id == id {
			return ks, nil
		}
	}

	return nil, fmt.Errorf("KeyServer %s not found", id)
}

type App struct {
	State  *Store
	Reader *ehclient.Reader
	Writer eh.Writer
}

func LoadUntilRealtime(
	ctx context.Context,
	client *ehclient.SystemClient,
) (*App, error) {
	store := New()

	a := &App{
		store,
		ehclient.NewReader(
			store,
			client,
			client.Logger("ehsettings.Reader")),
		client.EventLog}

	if err := a.Reader.LoadUntilRealtime(ctx); err != nil {
		return nil, err
	}

	return a, nil
}

var (
	lockAndUnlock = syncutil.LockAndUnlock // shorthand
)
