package ehserver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/system/ehpubsubdomain"
	"github.com/function61/gokit/cryptorandombytes"
	"github.com/function61/gokit/logex"
)

type publish struct {
	topic        string
	msg          []byte
}

type mqttNotifier struct {
	publishCh chan publish
	config    ehpubsubdomain.MqttConfigUpdated
	logl      *logex.Leveled
}

func newMqttNotifier(
	config ehpubsubdomain.MqttConfigUpdated,
	start func(task func(context.Context) error),
	logger *log.Logger,
) SubscriptionNotifier {
	publishCh := make(chan publish, 100)

	m := &mqttNotifier{
		publishCh: publishCh,
		config:    config,
		logl:      logex.Levels(logger),
	}

	start(func(ctx context.Context) error {
		return m.task(ctx)
	})

	return m
}

func (l *mqttNotifier) task(ctx context.Context) error {
	// TODO: reconnects?
	client, err := MqttClientFrom(&l.config, l.logl.Original)
	if err != nil {
		return err
	}
	defer client.Disconnect(250) // doesn't offer error status :O

	for {
		select {
		case <-ctx.Done():
			return nil
		case pub := <-l.publishCh:
			if err := WaitToken(client.Publish(pub.topic, MqttQos0AtMostOnce, false, pub.msg)); err != nil {
				return err
			}
		}
	}
}

func (l *mqttNotifier) NotifySubscriberOfActivity(
	ctx context.Context,
	subscription eh.SubscriptionId,
	appendResult eh.AppendResult,
) error {
	msg, err := json.Marshal(eh.MqttActivityNotification{
		Activity: []eh.CursorCompact{{appendResult.Cursor}},
	})
	if err != nil {
		return err
	}

	select {
	case l.publishCh <- publish{
		topic: MqttTopicForSubscription(subscription),
		msg:   msg,
	}: // non-blocking send
		return nil
	default:
		return fmt.Errorf(
			"NotifySubscriberOfActivity: failed to queue notification for %s b/c queue is full",
			appendResult.Cursor.Serialize())
	}
}

// dev/_/sub/foo
func MqttTopicForSubscription(subscription eh.SubscriptionId) string {
	return fmt.Sprintf("dev%s", subscription.StreamName().String())
}

func MqttClientFrom(conf *ehpubsubdomain.MqttConfigUpdated, logger *log.Logger) (mqtt.Client, error) {
	logl := logex.Levels(logger)
	clientCert, err := tls.X509KeyPair(
		[]byte(conf.ClientCertAuthCert),
		[]byte(conf.ClientCertAuthPrivateKey))
	if err != nil {
		return nil, fmt.Errorf("X509KeyPair: %w", err)
	}

	// Amazon uses this for access control and to prevent one client from having multiple
	// simultaneous connections, so we'll need to randomize this b/c we want multiple connections
	clientId := fmt.Sprintf("eh-%s", cryptorandombytes.Base64Url(8))

	opts := mqtt.NewClientOptions().
		AddBroker(conf.Endpoint).
		SetClientID(clientId).
		SetTLSConfig(clientCertAuth(clientCert))
	opts.OnConnectionLost = func(_ mqtt.Client, err error) { // FIXME
		logl.Error.Printf("connection lost: %v", err)
	}

	client := mqtt.NewClient(opts)

	if err := WaitToken(client.Connect()); err != nil {
		return nil, err
	}

	return client, nil
}
