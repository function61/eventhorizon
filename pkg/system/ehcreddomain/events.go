// Structure of data for all state changes
package ehcreddomain

import (
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/randomid"
)

var (
	NewCredentialId = randomid.Short
	NewPolicyId     = randomid.Short
)

var Types = ehevent.Types{
	"credential.Created":        func() ehevent.Event { return &CredentialCreated{} },
	"credential.Revoked":        func() ehevent.Event { return &CredentialRevoked{} },
	"credential.PolicyAttached": func() ehevent.Event { return &CredentialPolicyAttached{} },
	"credential.PolicyDetached": func() ehevent.Event { return &CredentialPolicyDetached{} },
	"policy.Created":            func() ehevent.Event { return &PolicyCreated{} },
	"policy.Renamed":            func() ehevent.Event { return &PolicyRenamed{} },
	"policy.ContentUpdated":     func() ehevent.Event { return &PolicyContentUpdated{} },
	"policy.Removed":            func() ehevent.Event { return &PolicyRemoved{} },
}

// ------

type CredentialCreated struct {
	meta   ehevent.EventMeta
	Id     string
	Name   string
	ApiKey string
}

func (e *CredentialCreated) MetaType() string         { return "credential.Created" }
func (e *CredentialCreated) Meta() *ehevent.EventMeta { return &e.meta }

func NewCredentialCreated(
	id string,
	name string,
	apiKey string,
	meta ehevent.EventMeta,
) *CredentialCreated {
	return &CredentialCreated{
		meta:   meta,
		Id:     id,
		Name:   name,
		ApiKey: apiKey,
	}
}

// ------

type CredentialRevoked struct {
	meta   ehevent.EventMeta
	Id     string
	Reason string
}

func (e *CredentialRevoked) MetaType() string         { return "credential.Revoked" }
func (e *CredentialRevoked) Meta() *ehevent.EventMeta { return &e.meta }

func NewCredentialRevoked(
	id string,
	reason string,
	meta ehevent.EventMeta,
) *CredentialRevoked {
	return &CredentialRevoked{
		meta:   meta,
		Id:     id,
		Reason: reason,
	}
}

// ------

type CredentialPolicyAttached struct {
	meta   ehevent.EventMeta
	Id     string
	Policy string
}

func (e *CredentialPolicyAttached) MetaType() string         { return "credential.PolicyAttached" }
func (e *CredentialPolicyAttached) Meta() *ehevent.EventMeta { return &e.meta }

func NewCredentialPolicyAttached(
	id string,
	policy string,
	meta ehevent.EventMeta,
) *CredentialPolicyAttached {
	return &CredentialPolicyAttached{
		meta:   meta,
		Id:     id,
		Policy: policy,
	}
}

// ------

type CredentialPolicyDetached struct {
	meta   ehevent.EventMeta
	Id     string
	Policy string
}

func (e *CredentialPolicyDetached) MetaType() string         { return "credential.PolicyDetached" }
func (e *CredentialPolicyDetached) Meta() *ehevent.EventMeta { return &e.meta }

func NewCredentialPolicyDetached(
	id string,
	policy string,
	meta ehevent.EventMeta,
) *CredentialPolicyDetached {
	return &CredentialPolicyDetached{
		meta:   meta,
		Id:     id,
		Policy: policy,
	}
}

// ------

type PolicyCreated struct {
	meta    ehevent.EventMeta
	Id      string
	Name    string
	Content string
}

func (e *PolicyCreated) MetaType() string         { return "policy.Created" }
func (e *PolicyCreated) Meta() *ehevent.EventMeta { return &e.meta }

func NewPolicyCreated(
	id string,
	name string,
	policy string,
	meta ehevent.EventMeta,
) *PolicyCreated {
	return &PolicyCreated{
		meta:    meta,
		Id:      id,
		Name:    name,
		Content: policy,
	}
}

// ------

type PolicyRenamed struct {
	meta ehevent.EventMeta
	Id   string
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
		Id:   id,
		Name: name,
	}
}

// ------

type PolicyContentUpdated struct {
	meta    ehevent.EventMeta
	Id      string
	Content string
}

func (e *PolicyContentUpdated) MetaType() string         { return "policy.ContentUpdated" }
func (e *PolicyContentUpdated) Meta() *ehevent.EventMeta { return &e.meta }

func NewPolicyContentUpdated(
	id string,
	content string,
	meta ehevent.EventMeta,
) *PolicyContentUpdated {
	return &PolicyContentUpdated{
		meta:    meta,
		Id:      id,
		Content: content,
	}
}

// ------

type PolicyRemoved struct {
	meta ehevent.EventMeta
	Id   string
}

func (e *PolicyRemoved) MetaType() string         { return "policy.Removed" }
func (e *PolicyRemoved) Meta() *ehevent.EventMeta { return &e.meta }

func NewPolicyRemoved(
	id string,
	meta ehevent.EventMeta,
) *PolicyRemoved {
	return &PolicyRemoved{
		meta: meta,
		Id:   id,
	}
}
