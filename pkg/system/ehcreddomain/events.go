// Structure of data for all state changes
package ehcreddomain

import (
	"github.com/function61/eventhorizon/pkg/ehevent"
)

var Types = ehevent.Types{
	"credential.Created": func() ehevent.Event { return &CredentialCreated{} },
	"credential.Revoked": func() ehevent.Event { return &CredentialRevoked{} },
}

// ------

type CredentialCreated struct {
	meta   ehevent.EventMeta
	Id     string
	Name   string
	ApiKey string
	Policy string
}

func (e *CredentialCreated) MetaType() string         { return "credential.Created" }
func (e *CredentialCreated) Meta() *ehevent.EventMeta { return &e.meta }

func NewCredentialCreated(
	id string,
	name string,
	apiKey string,
	policy string,
	meta ehevent.EventMeta,
) *CredentialCreated {
	return &CredentialCreated{
		meta:   meta,
		Id:     id,
		Name:   name,
		ApiKey: apiKey,
		Policy: policy,
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
