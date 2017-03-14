package main

import (
	"flag"
	"github.com/function61/pyramid/cli"
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/pubsub/server"
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
		pubSubServer := server.New(config.NewContext())

		log.Println(cli.WaitForInterrupt())

		pubSubServer.Close()
	}
}
