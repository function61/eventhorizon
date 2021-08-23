// Structure of data for all state changes
package ehcreddomain

import (
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/policy"
	"github.com/function61/eventhorizon/pkg/randomid"
)

var (
	NewAccessTokenID = randomid.Shorter // average user is expected to have 2 active access tokens
	NewUserID        = randomid.Short
	NewPolicyID      = randomid.Short
)

type PolicyKind string

// leaving room for "inline" (= per-user) policies
const (
	PolicyKindStandalone PolicyKind = "standalone"
)

var Types = ehevent.Types{
	"user.Created":            func() ehevent.Event { return &UserCreated{} },
	"user.AccessTokenCreated": func() ehevent.Event { return &UserAccessTokenCreated{} },
	"user.AccessTokenRevoked": func() ehevent.Event { return &UserAccessTokenRevoked{} },
	"user.PolicyAttached":     func() ehevent.Event { return &UserPolicyAttached{} },
	"user.PolicyDetached":     func() ehevent.Event { return &UserPolicyDetached{} },
	"policy.Created":          func() ehevent.Event { return &PolicyCreated{} },
	"policy.Renamed":          func() ehevent.Event { return &PolicyRenamed{} },
	"policy.ContentUpdated":   func() ehevent.Event { return &PolicyContentUpdated{} },
	"policy.Removed":          func() ehevent.Event { return &PolicyRemoved{} },
}

// ------

type UserCreated struct {
	meta ehevent.EventMeta
	ID   string
	Name string
}

func (e *UserCreated) MetaType() string         { return "user.Created" }
func (e *UserCreated) Meta() *ehevent.EventMeta { return &e.meta }

func NewUserCreated(
	id string,
	name string,
	meta ehevent.EventMeta,
) *UserCreated {
	return &UserCreated{
		meta: meta,
		ID:   id,
		Name: name,
	}
}

// ------

type UserAccessTokenCreated struct {
	meta   ehevent.EventMeta
	User   string
	ID     string // human-readable label to identify user's multiple access tokens
	Secret string
}

func (e *UserAccessTokenCreated) MetaType() string         { return "user.AccessTokenCreated" }
func (e *UserAccessTokenCreated) Meta() *ehevent.EventMeta { return &e.meta }

func NewUserAccessTokenCreated(
	user string,
	id string,
	secret string,
	meta ehevent.EventMeta,
) *UserAccessTokenCreated {
	return &UserAccessTokenCreated{
		meta:   meta,
		User:   user,
		ID:     id,
		Secret: secret,
	}
}

// ------

type UserAccessTokenRevoked struct {
	meta   ehevent.EventMeta
	User   string
	ID     string
	Reason string
}

func (e *UserAccessTokenRevoked) MetaType() string         { return "user.AccessTokenRevoked" }
func (e *UserAccessTokenRevoked) Meta() *ehevent.EventMeta { return &e.meta }

func NewUserAccessTokenRevoked(
	user string,
	id string,
	reason string,
	meta ehevent.EventMeta,
) *UserAccessTokenRevoked {
	return &UserAccessTokenRevoked{
		meta:   meta,
		User:   user,
		ID:     id,
		Reason: reason,
	}
}

// ------

type UserPolicyAttached struct {
	meta   ehevent.EventMeta
	User   string
	Policy string
}

func (e *UserPolicyAttached) MetaType() string         { return "user.PolicyAttached" }
func (e *UserPolicyAttached) Meta() *ehevent.EventMeta { return &e.meta }

func NewUserPolicyAttached(
	id string,
	policy string,
	meta ehevent.EventMeta,
) *UserPolicyAttached {
	return &UserPolicyAttached{
		meta:   meta,
		User:   id,
		Policy: policy,
	}
}

// ------

type UserPolicyDetached struct {
	meta   ehevent.EventMeta
	User   string
	Policy string
}

func (e *UserPolicyDetached) MetaType() string         { return "user.PolicyDetached" }
func (e *UserPolicyDetached) Meta() *ehevent.EventMeta { return &e.meta }

func NewUserPolicyDetached(
	id string,
	policy string,
	meta ehevent.EventMeta,
) *UserPolicyDetached {
	return &UserPolicyDetached{
		meta:   meta,
		User:   id,
		Policy: policy,
	}
}

// ------

type PolicyCreated struct {
	meta    ehevent.EventMeta
	ID      string
	Kind    PolicyKind
	Name    string
	Content policy.Policy // pretty safe b/c we can always switch to json.RawMessage if we want to decouple
}

func (e *PolicyCreated) MetaType() string         { return "policy.Created" }
func (e *PolicyCreated) Meta() *ehevent.EventMeta { return &e.meta }

func NewPolicyCreated(
	id string,
	kind PolicyKind,
	name string,
	policy policy.Policy,
	meta ehevent.EventMeta,
) *PolicyCreated {
	return &PolicyCreated{
		meta:    meta,
		ID:      id,
		Kind:    kind,
		Name:    name,
		Content: policy,
	}
}

// ------

type PolicyRenamed struct {
	meta ehevent.EventMeta
	ID   string
	Name string
}

func (e *PolicyRenamed) MetaType() string         { return "policy.Renamed" }
func (e *PolicyRenamed) Meta() *ehevent.EventMeta { return &e.meta }

func NewPolicyRenamed(
	id string,
	name string,
	meta ehevent.EventMeta,
) *PolicyRenamed {
	return &PolicyRenamed{
		meta: meta,
		ID:   id,
		Name: name,
	}
}

// ------

type PolicyContentUpdated struct {
	meta    ehevent.EventMeta
	ID      string
	Content policy.Policy
}

func (e *PolicyContentUpdated) MetaType() string         { return "policy.ContentUpdated" }
func (e *PolicyContentUpdated) Meta() *ehevent.EventMeta { return &e.meta }

func NewPolicyContentUpdated(
	id string,
	content policy.Policy,
	meta ehevent.EventMeta,
) *PolicyContentUpdated {
	return &PolicyContentUpdated{
		meta:    meta,
		ID:      id,
		Content: content,
	}
}

// ------

type PolicyRemoved struct {
	meta ehevent.EventMeta
	ID   string
}

func (e *PolicyRemoved) MetaType() string         { return "policy.Removed" }
func (e *PolicyRemoved) Meta() *ehevent.EventMeta { return &e.meta }

func NewPolicyRemoved(
	id string,
	meta ehevent.EventMeta,
) *PolicyRemoved {
	return &PolicyRemoved{
		meta: meta,
		ID:   id,
	}
}
