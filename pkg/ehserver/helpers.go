package ehserver

import (
	"crypto/tls"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// mind boggling that we've to declare these ourselves
const (
	MqttQos0AtMostOnce  = byte(0)
	MqttQos1LeastOnce   = byte(1)
	MqttQos2ExactlyOnce = byte(2)
)

func WaitToken(t mqtt.Token) error {
	t.Wait()
	return t.Error()
}

func clientCertAuth(clientCert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{clientCert},
	}
}
