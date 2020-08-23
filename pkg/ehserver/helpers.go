package ehserver

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// mind boggling that we've to declare these ourselves
const (
	MqttQos0AtMostOnce  = byte(0)
	mqttQos1LeastOnce   = byte(1)
	mqttQos2ExactlyOnce = byte(2)
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

func respondJson(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// TODO: move to gokit
func lockAndUnlock(mu *sync.Mutex) func() {
	mu.Lock()

	return func() {
		mu.Unlock()
	}
}
