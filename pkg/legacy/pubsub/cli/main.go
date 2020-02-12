package main

import (
	"flag"
	"github.com/function61/eventhorizon/pkg/legacy/config/configfactory"
	"github.com/function61/eventhorizon/pkg/legacy/pubsub/server"
	"github.com/function61/eventhorizon/pkg/legacy/util/clicommon"
	"log"
)

/*	Pub/sub, requirements:

	- (done) Reconnect on errors
	- (done) Full duplex protocol
	- (done) Keepalive
	- (later) Authentication via TLS
*/

func main() {
	subTopic := flag.String("sub-topic", "", "sub-topic")

	pubTopic := flag.String("pub-topic", "", "pub-topic")
	pubMessage := flag.String("pub-msg", "", "pub-msg")

	flag.Parse()

	if *pubTopic != "" {
		testPublish(*pubTopic, *pubMessage)
	} else if *subTopic != "" {
		startSubscriber(*subTopic)
	} else {
		pubSubServer := server.New(configfactory.BuildMust())

		log.Println(clicommon.WaitForInterrupt())

		pubSubServer.Close()
	}
}
