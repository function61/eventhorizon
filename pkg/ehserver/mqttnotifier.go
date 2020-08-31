package ehserver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iotdataplane"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/system/ehpubsubdomain"
	"github.com/function61/gokit/cryptorandombytes"
	"github.com/function61/gokit/logex"
)

type publish struct {
	topic string
	msg   []byte
}

type mqttNotifier struct {
	publishCh chan publish
	config    ehpubsubdomain.MqttConfigUpdated
	logl      *logex.Leveled
	iot       *iotdataplane.IoTDataPlane
}

func newMqttNotifier(
	config ehpubsubdomain.MqttConfigUpdated,
	start func(task func(context.Context) error),
	logger *log.Logger,
) SubscriptionNotifier {
	publishCh := make(chan publish, 100)

	iot := func() *iotdataplane.IoTDataPlane {
		if strings.Contains(config.Endpoint, "-ats.iot.") && strings.Contains(config.Endpoint, ".amazonaws.com") {
			endpointUrl, err := url.Parse(config.Endpoint)
			if err != nil {
				panic(err)
			}

			return iotdataplane.New(session.Must(session.NewSession()), aws.NewConfig().WithEndpoint(endpointUrl.Hostname()))
		} else {
			return nil
		}
	}()

	m := &mqttNotifier{
		publishCh: publishCh,
		config:    config,
		logl:      logex.Levels(logger),
		iot:       iot,
	}

	start(func(ctx context.Context) error {
		if m.iot == nil {
			// why not just use MQTT everywhere? this seems to be more reliable in AWS Lambda.
			// https://github.com/eclipse/paho.mqtt.golang/issues/317
			// https://github.com/eclipse/paho.mqtt.golang/issues/72
			return m.taskMqtt(ctx)
		} else {
			return m.taskAwsIotDataplane(ctx)
		}
	})

	return m
}

// uses actual MQTT
func (l *mqttNotifier) taskMqtt(ctx context.Context) error {
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
				l.logl.Error.Printf("Publish: %v", err)
			}
		}
	}
}

// delivers MQTT publishes via IoT dataplane (probably a HTTP front)
func (l *mqttNotifier) taskAwsIotDataplane(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case pub := <-l.publishCh:
			if _, err := l.iot.PublishWithContext(ctx, &iotdataplane.PublishInput{
				Topic:   &pub.topic,
				Qos:     aws.Int64(0),
				Payload: pub.msg,
			}); err != nil {
				l.logl.Error.Printf("Publish: %v", err)
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
	case l.publishCh <- publish{ // non-blocking send
		topic: MqttTopicForSubscription(subscription),
		msg:   msg,
	}:
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
