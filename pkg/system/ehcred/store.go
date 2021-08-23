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

type User struct {
	ID         string
	Created    time.Time
	Name       string
	PolicyIDs  []string
	AccessKeys []AccessKey  // usually has one, but during rotation there are expected to be two active ones
	AuditLog   []AuditEntry // TODO: limit maximum # of items
}

type AuditEntry struct {
	Time    time.Time
	Message string
}

type AccessKey struct {
	ID      string
	Created time.Time
	Secret  string
}

func (a AccessKey) CombinedToken() string {
	// adding access key's ID in front of it to make it easier for the user to identify which
	// access key their program was configured with.
	//
	// AWS has these separate (access key id + access key secret), but I feel combining these in a
	// single string is easier to manage for the user.
	return fmt.Sprintf("%s.%s", a.ID, a.Secret)
}

// credential is policy-merged view of internalCredential that is consumable without
// having to have knowledge of inline/attachable policies
type Credential struct {
	UserID  string
	ID      string
	Name    string
	Created time.Time
	ApiKey  string
	Policy  policy.Policy
}

type computedCredential struct {
	UserID       string
	AccessKeyID  string
	MergedPolicy policy.Policy
}

type Policy struct {
	ID      string
	Name    string
	Created time.Time
	Content policy.Policy
}

type stateFormat struct {
	Users    []*User
	Policies map[string]*Policy
}

func newStateFormat() stateFormat {
	return stateFormat{
		Users:    []*User{},
		Policies: map[string]*Policy{},
	}
}

type Store struct {
	version      eh.Cursor
	mu           sync.Mutex
	state        stateFormat // for easy snapshotting
	cachedLookup map[string]*computedCredential
}

func New() *Store {
	return &Store{
		version:      eh.SysCredentials.Beginning(),
		state:        newStateFormat(),
		cachedLookup: map[string]*computedCredential{},
	}
}

func (s *Store) CredentialByCombinedToken(apiKey string) *computedCredential {
	defer lockAndUnlock(&s.mu)()

	return s.cachedLookup[apiKey]
}

func (s *Store) UserByID(id string) *User {
	defer lockAndUnlock(&s.mu)()

	return s.userByID(id)
}

func (s *Store) Users() []User {
	defer lockAndUnlock(&s.mu)()

	users := []User{}
	for _, user := range s.state.Users {
		users = append(users, *user)
	}

	return users
}

func (s *Store) PolicyByID(id string) *Policy {
	defer lockAndUnlock(&s.mu)()

	return s.state.Policies[id]
}

func (s *Store) Policies() []Policy {
	defer lockAndUnlock(&s.mu)()

	policies := []Policy{}
	for _, pol := range s.state.Policies {
		policies = append(policies, *pol)
	}

	sort.Slice(policies, func(i, j int) bool { return policies[i].ID < policies[j].ID })

	return policies
}

func (s *Store) PolicyExistsAndIsAbleToDelete(id string) error {
	defer lockAndUnlock(&s.mu)()

	if _, exists := s.state.Policies[id]; !exists {
		return fmt.Errorf("policy '%s' does not exist", id)
	}

	for _, user := range s.state.Users {
		if sliceutil.ContainsString(user.PolicyIDs, id) {
			return fmt.Errorf("user '%s' has policy '%s' attached", user.ID, id)
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

	if err := json.Unmarshal(snap.Data, &s.state); err != nil {
		return err
	}

	s.rebuildComputedCredentialLookup()

	return nil
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
	case *ehcreddomain.UserCreated:
		s.state.Users = append(s.state.Users, &User{
			ID:         e.ID,
			Created:    e.Meta().Time(),
			Name:       e.Name,
			PolicyIDs:  []string{},
			AccessKeys: []AccessKey{},
			AuditLog: []AuditEntry{
				audit(e, "Created"),
			},
		})
	case *ehcreddomain.UserAccessTokenCreated:
		user := s.userByID(e.User)

		user.AccessKeys = append(user.AccessKeys, AccessKey{
			ID:      e.ID,
			Created: e.Meta().Time(),
			Secret:  e.Secret,
		})

		s.rebuildComputedCredentialLookup()
		user.AuditLog = append(user.AuditLog, audit(e, "Created access token "+e.ID))
	case *ehcreddomain.UserAccessTokenRevoked:
		user := s.userByID(e.User)

		keep := []AccessKey{}
		for _, key := range user.AccessKeys {
			if key.ID != e.ID {
				keep = append(keep, key)
			}
		}

		user.AccessKeys = keep
		s.rebuildComputedCredentialLookup()
		user.AuditLog = append(user.AuditLog, audit(e, "Revoked access token "+e.ID))
	case *ehcreddomain.UserPolicyAttached:
		user := s.userByID(e.User)

		user.PolicyIDs = append(user.PolicyIDs, e.Policy)

		s.rebuildComputedCredentialLookup()
		user.AuditLog = append(user.AuditLog, audit(e, "Attached policy "+e.Policy))
	case *ehcreddomain.UserPolicyDetached:
		user := s.userByID(e.User)

		user.PolicyIDs = sliceutil.FilterString(user.PolicyIDs, func(id string) bool { return id != e.Policy })

		s.rebuildComputedCredentialLookup()
		user.AuditLog = append(user.AuditLog, audit(e, "Detached policy "+e.Policy))
	case *ehcreddomain.PolicyCreated:
		s.state.Policies[e.ID] = &Policy{
			ID:      e.ID,
			Name:    e.Name,
			Created: e.Meta().Time(),
			Content: e.Content,
		}
	case *ehcreddomain.PolicyRenamed:
		s.state.Policies[e.ID].Name = e.Name
	case *ehcreddomain.PolicyContentUpdated:
		s.state.Policies[e.ID].Content = e.Content

		s.rebuildComputedCredentialLookup()
	case *ehcreddomain.PolicyRemoved:
		delete(s.state.Policies, e.ID)
	default:
		return ehclient.UnsupportedEventTypeErr(ev)
	}

	return nil
}

func (s *Store) rebuildComputedCredentialLookup() {
	cache := map[string]*computedCredential{}

	for _, user := range s.state.Users {
		for _, accessKey := range user.AccessKeys {
			policies := []policy.Policy{}
			for _, policyId := range user.PolicyIDs {
				attachedPolicy, found := s.state.Policies[policyId]
				if !found {
					panic(fmt.Errorf("attached policy '%s' not found", policyId))
				}

				policies = append(policies, attachedPolicy.Content)
			}

			cache[accessKey.CombinedToken()] = &computedCredential{
				UserID:       user.ID,
				AccessKeyID:  accessKey.ID,
				MergedPolicy: policy.Merge(policies...),
			}
		}
	}

	s.cachedLookup = cache
}

func (s *Store) userByID(id string) *User {
	for _, user := range s.state.Users {
		if user.ID == id {
			return user
		}
	}

	return nil
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

func audit(e ehevent.Event, msg string) AuditEntry {
	return AuditEntry{e.Meta().Time(), msg}
}
