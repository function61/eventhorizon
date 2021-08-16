// State for access control: credentials (= API keys), policies (= authorizations)
package ehcred

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/policy"
	"github.com/function61/eventhorizon/pkg/system/ehcreddomain"
	"github.com/function61/gokit/sliceutil"
	"github.com/function61/gokit/sync/syncutil"
)

// credential is policy-merged view of internalCredential that is consumable without
// having to have knowledge of inline/attachable policies
type Credential struct {
	Id      string
	Name    string
	Created time.Time
	ApiKey  string
	Policy  policy.Policy
}

type internalCredential struct {
	Id       string
	Name     string
	Created  time.Time
	Policies []string
}

type Policy struct {
	Id      string
	Name    string
	Created time.Time
	Content policy.Policy
}

type stateFormat struct {
	Credentials map[string]*internalCredential
	Policies    map[string]*Policy
}

func newStateFormat() stateFormat {
	return stateFormat{
		Credentials: map[string]*internalCredential{},
		Policies:    map[string]*Policy{},
	}
}

type Store struct {
	version eh.Cursor
	mu      sync.Mutex
	state   stateFormat // for easy snapshotting
}

func New() *Store {
	return &Store{
		version: eh.SysCredentials.Beginning(),
		state:   newStateFormat(),
	}
}

func (s *Store) CredentialByApiKey(apiKey string) *Credential {
	defer lockAndUnlock(&s.mu)()

	if c, found := s.state.Credentials[apiKey]; found {
		return s.credentialFromInternal(c, apiKey)
	} else {
		return nil
	}
}

// needed b/c of weird'ish stateFormat
func (s *Store) CredentialById(id string) (*Credential, error) {
	defer lockAndUnlock(&s.mu)()

	for apiKey, cred := range s.state.Credentials {
		if cred.Id == id {
			return s.credentialFromInternal(cred, apiKey), nil
		}
	}

	return nil, fmt.Errorf("API key for credential '%s' not found", id)
}

func (s *Store) Credentials() []Credential {
	defer lockAndUnlock(&s.mu)()

	credentials := []Credential{}
	for apiKey, cred := range s.state.Credentials {
		credentials = append(credentials, *s.credentialFromInternal(cred, apiKey))
	}

	sort.Slice(credentials, func(i, j int) bool { return credentials[i].Id < credentials[j].Id })

	return credentials
}

func (s *Store) Policies() []Policy {
	defer lockAndUnlock(&s.mu)()

	policies := []Policy{}
	for _, pol := range s.state.Policies {
		policies = append(policies, *pol)
	}

	sort.Slice(policies, func(i, j int) bool { return policies[i].Id < policies[j].Id })

	return policies
}

// returns error if credential does not exist
func (s *Store) CredentialAttachedPolicyIds(credentialId string) ([]string, error) {
	defer lockAndUnlock(&s.mu)()

	for _, cred := range s.state.Credentials {
		if cred.Id == credentialId {
			return cred.Policies, nil
		}
	}

	return nil, fmt.Errorf("credential '%s' does not exist", credentialId)
}

func (s *Store) PolicyExistsAndIsAbleToDelete(id string) error {
	defer lockAndUnlock(&s.mu)()

	if _, exists := s.state.Policies[id]; !exists {
		return fmt.Errorf("policy '%s' does not exist", id)
	}

	for _, cred := range s.state.Credentials {
		if sliceutil.ContainsString(cred.Policies, id) {
			return fmt.Errorf("credential '%s' has policy '%s' attached", cred.Id, id)
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
	return eh.NewV1Perspective("eh.credentials") // change if persisted stateFormat changes in backwards-incompat way
}

func (s *Store) GetEventTypes() []ehclient.LogDataKindDeserializer {
	return ehclient.EncryptedDataDeserializer(ehcreddomain.Types)
}

func (s *Store) ProcessEvents(_ context.Context, processAndCommit ehclient.EventProcessorHandler) error {
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
	case *ehcreddomain.CredentialCreated:
		s.state.Credentials[e.ApiKey] = &internalCredential{
			Id:       e.Id,
			Name:     e.Name,
			Created:  e.Meta().Time(),
			Policies: []string{},
		}
	case *ehcreddomain.CredentialRevoked:
		for key, cred := range s.state.Credentials {
			if cred.Id == e.Id {
				delete(s.state.Credentials, key)
				break
			}
		}
	case *ehcreddomain.CredentialPolicyAttached:
		cred, err := s.credentialById(e.Id)
		if err != nil {
			return err
		}

		cred.Policies = append(cred.Policies, e.Policy)
	case *ehcreddomain.CredentialPolicyDetached:
		cred, err := s.credentialById(e.Id)
		if err != nil {
			return err
		}

		cred.Policies = sliceutil.FilterString(cred.Policies, func(id string) bool { return id != e.Policy })
	case *ehcreddomain.PolicyCreated:
		s.state.Policies[e.Id] = &Policy{
			Id:      e.Id,
			Name:    e.Name,
			Created: e.Meta().Time(),
			Content: e.Content,
		}
	case *ehcreddomain.PolicyRenamed:
		s.state.Policies[e.Id].Name = e.Name
	case *ehcreddomain.PolicyContentUpdated:
		s.state.Policies[e.Id].Content = e.Content
	case *ehcreddomain.PolicyRemoved:
		delete(s.state.Policies, e.Id)
	default:
		return ehclient.UnsupportedEventTypeErr(ev)
	}

	return nil
}

func (s *Store) credentialById(id string) (*internalCredential, error) {
	for _, cred := range s.state.Credentials {
		if cred.Id == id {
			return cred, nil
		}
	}

	return nil, fmt.Errorf("API key for credential '%s' not found", id)
}

// merges inline/attachable policies to a simpler, public credential object
func (s *Store) credentialFromInternal(cred *internalCredential, apiKey string) *Credential {
	policies := []policy.Policy{}
	for _, policyId := range cred.Policies {
		attachedPolicy, found := s.state.Policies[policyId]
		if !found {
			panic(fmt.Errorf("attached policy '%s' not found", policyId))
		}

		policies = append(policies, attachedPolicy.Content)
	}

	return &Credential{
		Id:      cred.Id,
		Name:    cred.Name,
		Created: cred.Created,
		ApiKey:  apiKey,
		Policy:  policy.Merge(policies...),
	}
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
			client),
		client.EventLog}

	if err := a.Reader.LoadUntilRealtime(ctx); err != nil {
		return nil, err
	}

	return a, nil
}

var (
	lockAndUnlock = syncutil.LockAndUnlock // shorthand
)
