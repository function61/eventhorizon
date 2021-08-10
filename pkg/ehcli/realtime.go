package ehcli

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehclientfactory"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/ehserver"
	"github.com/function61/eventhorizon/pkg/system/ehsettings"
	"github.com/function61/eventhorizon/pkg/system/ehsettingsdomain"
	"github.com/function61/eventhorizon/pkg/system/ehsubscription"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/os/osutil"
	"github.com/spf13/cobra"
)

var (
	waitToken          = ehserver.WaitToken
	mqttClientFrom     = ehserver.MqttClientFrom
	mqttQos0AtMostOnce = ehserver.MqttQos0AtMostOnce
)

func realtimeEntrypoint() *cobra.Command {
	parentCmd := &cobra.Command{
		Use:   "realtime",
		Short: "Realtime subsystem (MQTT Pub/Sub) management",
	}

	parentCmd.AddCommand(&cobra.Command{
		Use:   "sub [subscriptionId]",
		Short: "Subscribe (also works as connectivity check)",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(mqttSubscribe(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "config-update [namespace] [endpoint] [auth-cert-path] [auth-cert-key-path]",
		Short: "Update MQTT configuration",
		Args:  cobra.ExactArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(mqttConfigUpdate(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[1],
				args[2],
				args[3],
				args[0],
				true,
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "config-cat",
		Short: "Display config",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(mqttConfigDisplay(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				rootLogger))
		},
	})

	return parentCmd
}

func mqttSubscribe(ctx context.Context, subscriptionIdRaw string, logger *log.Logger) error {
	subscriptionId := eh.NewSubscriberID(subscriptionIdRaw)

	logl := logex.Levels(logger)

	ehClient, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	// check that the subscription exists
	if _, err := ehsubscription.LoadUntilRealtime(
		ctx,
		subscriptionId,
		ehClient,
		ehsubscription.GlobalCache,
		logger,
	); err != nil {
		return err
	}

	sysState, err := ehsettings.LoadUntilRealtime(ctx, ehClient)
	if err != nil {
		return err
	}

	mqttConfig := sysState.State.MqttConfig()
	if mqttConfig == nil {
		return errors.New("no config set")
	}

	mqClient, err := mqttClientFrom(mqttConfig, logger)
	if err != nil {
		return err
	}
	defer mqClient.Disconnect(250) // doesn't offer error status :O

	incomingMsg := make(chan eh.MqttActivityNotification)

	topic := ehserver.MqttTopicForSubscription(subscriptionId, mqttConfig.Namespace)

	if err := waitToken(mqClient.Subscribe(topic, mqttQos0AtMostOnce, func(_ mqtt.Client, msg mqtt.Message) {
		activityNotifaction := eh.MqttActivityNotification{}
		if err := json.Unmarshal(msg.Payload(), &activityNotifaction); err != nil {
			logl.Error.Printf("Unmarshal: %v", err)
			return
		}

		incomingMsg <- activityNotifaction
	})); err != nil {
		return err
	}

	logl.Info.Printf("subscribed to %s; waiting for msg", topic)

	for {
		select {
		case <-ctx.Done():
			logl.Info.Println("graceful exit")
			return nil
		case msg := <-incomingMsg:
			for _, activity := range msg.Activity {
				logl.Info.Printf("activity %s", activity.Serialize())
			}
		}
	}
}

func mqttConfigUpdate(
	ctx context.Context,
	endpoint string,
	authCertPath string,
	authCertKeyPath string,
	namespace string,
	verifyConnectivity bool,
	logger *log.Logger,
) error {
	authCert, err := ioutil.ReadFile(authCertPath)
	if err != nil {
		return fmt.Errorf("authCertPath: %w", err)
	}

	authCertKey, err := ioutil.ReadFile(authCertKeyPath)
	if err != nil {
		return fmt.Errorf("authCertKeyPath: %w", err)
	}

	if _, err := tls.X509KeyPair(authCert, authCertKey); err != nil {
		return fmt.Errorf("X509KeyPair: %w", err)
	}

	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	sysState, err := ehsettings.LoadUntilRealtime(ctx, client)
	if err != nil {
		return err
	}

	configUpdated := ehsettingsdomain.NewMqttConfigUpdated(
		endpoint,
		string(authCert),
		string(authCertKey),
		namespace,
		ehevent.MetaSystemUser(time.Now()))

	if verifyConnectivity {
		if err := connectivityCheck(configUpdated, logger); err != nil {
			return fmt.Errorf("connectivityCheck: %w", err)
		}
	}

	if err := client.AppendAfter(
		ctx,
		sysState.State.Version(),
		configUpdated,
	); err != nil {
		return fmt.Errorf("mqttConfigUpdate: Writer: %w", err)
	}

	return nil
}

func mqttConfigDisplay(ctx context.Context, logger *log.Logger) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	sysState, err := ehsettings.LoadUntilRealtime(ctx, client)
	if err != nil {
		return err
	}

	mqttConfig := sysState.State.MqttConfig()
	if mqttConfig == nil {
		fmt.Println("no config set")
		return nil
	}

	fmt.Println(ehevent.Serialize(mqttConfig))
	return nil
}

func connectivityCheck(configUpdated *ehsettingsdomain.MqttConfigUpdated, logger *log.Logger) error {
	client, err := mqttClientFrom(configUpdated, logger)
	if err != nil {
		return err
	}
	defer client.Disconnect(250) // doesn't offer error status :O

	if err := waitToken(client.Publish("connectivity_check", mqttQos0AtMostOnce, false, "hello world")); err != nil {
		return fmt.Errorf("Publish: %w", err)
	}

	return nil
}
