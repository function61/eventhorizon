// Structure of data for all state changes
package ehsettingsdomain

import (
	"github.com/function61/eventhorizon/pkg/ehevent"
)

const (
	ClusterWideKeyId = "eh-cluster-wide-key"
)

var Types = ehevent.Types{
	"mqtt.ConfigUpdated":    func() ehevent.Event { return &MqttConfigUpdated{} },
	"keygroup.Created":      func() ehevent.Event { return &KeygroupCreated{} },
	"kek.Registered":        func() ehevent.Event { return &KekRegistered{} },
	"keyserver.Created":     func() ehevent.Event { return &KeyserverCreated{} },
	"keyserver.KeyAttached": func() ehevent.Event { return &KeyserverKeyAttached{} },
	"keyserver.KeyDetached": func() ehevent.Event { return &KeyserverKeyDetached{} },
}

// ------

type KeygroupCreated struct {
	meta          ehevent.EventMeta
	Id            string
	RsaPublicKeys []string
	Server        string
}

func (e *KeygroupCreated) MetaType() string         { return "keygroup.Created" }
func (e *KeygroupCreated) Meta() *ehevent.EventMeta { return &e.meta }

func NewKeygroupCreated(
	id string,
	rsaPublicKeys []string,
	server string,
	meta ehevent.EventMeta,
) *KeygroupCreated {
	return &KeygroupCreated{
		meta:          meta,
		Id:            id,
		RsaPublicKeys: rsaPublicKeys,
		Server:        server,
	}
}

// ------

type KekRegistered struct {
	meta      ehevent.EventMeta
	Id        string // same as envelopeenc's slot's KEK ID
	Kind      string // "rsa" | "nacl-secretbox"
	Label     string
	PublicKey string `json:"PublicKey,omitempty"` // used for RSA
}

func (e *KekRegistered) MetaType() string         { return "kek.Registered" }
func (e *KekRegistered) Meta() *ehevent.EventMeta { return &e.meta }

func NewKekRegistered(
	id string,
	kind string,
	label string,
	publicKey string,
	meta ehevent.EventMeta,
) *KekRegistered {
	return &KekRegistered{
		meta:      meta,
		Id:        id,
		Kind:      kind,
		Label:     label,
		PublicKey: publicKey,
	}
}

// ------

type KeyserverCreated struct {
	meta  ehevent.EventMeta
	ID    string
	Label string
}

func (e *KeyserverCreated) MetaType() string         { return "keyserver.Created" }
func (e *KeyserverCreated) Meta() *ehevent.EventMeta { return &e.meta }

func NewKeyserverCreated(
	id string,
	label string,
	meta ehevent.EventMeta,
) *KeyserverCreated {
	return &KeyserverCreated{
		meta:  meta,
		ID:    id,
		Label: label,
	}
}

// ------

type KeyserverKeyAttached struct {
	meta   ehevent.EventMeta
	Server string
	Key    string
}

func (e *KeyserverKeyAttached) MetaType() string         { return "keyserver.KeyAttached" }
func (e *KeyserverKeyAttached) Meta() *ehevent.EventMeta { return &e.meta }

func NewKeyserverKeyAttached(
	server string,
	key string,
	meta ehevent.EventMeta,
) *KeyserverKeyAttached {
	return &KeyserverKeyAttached{
		meta:   meta,
		Server: server,
		Key:    key,
	}
}

// ------

type KeyserverKeyDetached struct {
	meta   ehevent.EventMeta
	Server string
	Key    string
}

func (e *KeyserverKeyDetached) MetaType() string         { return "keyserver.KeyDetached" }
func (e *KeyserverKeyDetached) Meta() *ehevent.EventMeta { return &e.meta }

func NewKeyserverKeyDetached(
	server string,
	key string,
	meta ehevent.EventMeta,
) *KeyserverKeyDetached {
	return &KeyserverKeyDetached{
		meta:   meta,
		Server: server,
		Key:    key,
	}
}

// ------

type MqttConfigUpdated struct {
	meta                     ehevent.EventMeta
	Endpoint                 string // Looks like "tls://a11ynok4kobexy-ats.iot.eu-central-1.amazonaws.com:8883"
	ClientCertAuthCert       string // PEM-encoded X509 cert
	ClientCertAuthPrivateKey string // PEM-encoded private key
	Namespace                string // "prod" | "staging" | "dev" | ...
}

func (e *MqttConfigUpdated) MetaType() string         { return "mqtt.ConfigUpdated" }
func (e *MqttConfigUpdated) Meta() *ehevent.EventMeta { return &e.meta }

func NewMqttConfigUpdated(
	endpoint string,
	clientCertAuthCert string,
	clientCertAuthPrivateKey string,
	namespace string,
	meta ehevent.EventMeta,
) *MqttConfigUpdated {
	return &MqttConfigUpdated{
		meta:                     meta,
		Endpoint:                 endpoint,
		ClientCertAuthCert:       clientCertAuthCert,
		ClientCertAuthPrivateKey: clientCertAuthPrivateKey,
		Namespace:                namespace,
	}
}
