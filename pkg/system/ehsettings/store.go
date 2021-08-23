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
	Keks       []*KEK
	KeyGroups  []KeyGroup
}

type KeyServer struct {
	Id             string
	Created        time.Time
	Label          string
	URL            string
	AttachedKekIds []string
}

type KEK struct {
	Id         string
	Registered time.Time
	Kind       string
	Label      string
	PublicKey  string
}

// each stream has one associated key group.
// this group is consulted on stream creation and on DEK rotation.
type KeyGroup struct {
	ID   string
	Name string
	KEKs []string // which KEKs should control access to the stream's data
}

func newStateFormat() stateFormat {
	return stateFormat{
		KeyServers: []*KeyServer{},
		Keks:       []*KEK{},
		KeyGroups:  []KeyGroup{},
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

// used when creating new streams (or rotating DEKs for existing streams) to decide which KEKs shall
// get to control access to the stream.
func (s *Store) KeyGroupIDForStream(stream eh.StreamName) string {
	// TODO: we don't have enough of understanding for the needs of this, so we'll just use default
	//       for everything until we have enough of needs to know what to do.

	return "default"
}

func (s *Store) Data() stateFormat {
	defer lockAndUnlock(&s.mu)()

	return s.state
}

func (s *Store) KeyGroup(id string) *KeyGroup {
	defer lockAndUnlock(&s.mu)()

	for _, keyGroup := range s.state.KeyGroups {
		if keyGroup.ID == id {
			return &keyGroup
		}
	}

	return nil
}

func (s *Store) KEK(kekId string) *KEK {
	defer lockAndUnlock(&s.mu)()

	for _, kek := range s.state.Keks {
		if kek.Id == kekId {
			return kek
		}
	}

	return nil
}

func (s *Store) KEKs() []KEK {
	defer lockAndUnlock(&s.mu)()

	keks := []KEK{}
	for _, kek := range s.state.Keks {
		keks = append(keks, *kek)
	}

	return keks
}

func (s *Store) KeyServers() []KeyServer {
	defer lockAndUnlock(&s.mu)()

	keyServers := []KeyServer{}
	for _, keyServer := range s.state.KeyServers {
		keyServers = append(keyServers, *keyServer)
	}

	return keyServers
}

func (s *Store) KeyServerWithKEKAttached(kekId string) *KeyServer {
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

	return eh.NewSnapshot(s.version, data, s.Perspective()), nil
}

func (s *Store) Perspective() eh.SnapshotPerspective {
	return eh.NewV1Perspective("eh.settings") // change if persisted stateFormat changes in backwards-incompat way
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
		s.state.Keks = append(s.state.Keks, &KEK{
			Id:         e.ID,
			Registered: e.Meta().Time(),
			Kind:       e.Kind,
			Label:      e.Label,
			PublicKey:  e.PublicKey,
		})
	case *ehsettingsdomain.KeyserverCreated:
		s.state.KeyServers = append(s.state.KeyServers, &KeyServer{
			Id:             e.ID,
			Created:        e.Meta().Time(),
			Label:          e.Label,
			URL:            e.URL,
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
		s.state.KeyGroups = append(s.state.KeyGroups, KeyGroup{
			ID:   e.ID,
			Name: e.Name,
			KEKs: e.KEKs,
		})
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

var (
	appCache   *App
	appCacheMu sync.Mutex
)

func LoadUntilRealtime(
	ctx context.Context,
	client *ehclient.SystemClient,
) (*App, error) {
	defer lockAndUnlock(&appCacheMu)()

	// singleton use for caching since we only have one settings stream and this is used somewhat often
	if appCache != nil {
		return appCache, nil
	}

	store := New()

	a := &App{
		store,
		ehclient.NewReader(
			store,
			client),
		client.EventLog}

	appCache = a

	if err := a.Reader.LoadUntilRealtime(ctx); err != nil {
		return nil, err
	}

	return a, nil
}

var (
	lockAndUnlock = syncutil.LockAndUnlock // shorthand
)
