// Structure of data for all state changes
package ehpubsubdomain

import (
	"github.com/function61/eventhorizon/pkg/ehevent"
)

var Types = ehevent.Types{
	"mqtt.ConfigUpdated": func() ehevent.Event { return &MqttConfigUpdated{} },
}

// ------

type MqttConfigUpdated struct {
	meta                     ehevent.EventMeta
	Endpoint                 string // Looks like "tls://a11ynok4kobexy-ats.iot.eu-central-1.amazonaws.com:8883"
	ClientCertAuthCert       string // PEM-encoded X509 cert
	ClientCertAuthPrivateKey string // PEM-encoded private key
}

func (e *MqttConfigUpdated) MetaType() string         { return "mqtt.ConfigUpdated" }
func (e *MqttConfigUpdated) Meta() *ehevent.EventMeta { return &e.meta }

func NewMqttConfigUpdated(
	endpoint string,
	clientCertAuthCert string,
	clientCertAuthPrivateKey string,
	meta ehevent.EventMeta,
) *MqttConfigUpdated {
	return &MqttConfigUpdated{
		meta:                     meta,
		Endpoint:                 endpoint,
		ClientCertAuthCert:       clientCertAuthCert,
		ClientCertAuthPrivateKey: clientCertAuthPrivateKey,
	}
}
