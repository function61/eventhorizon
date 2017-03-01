package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"strconv"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/pubsub/server"
)

/*	Pub/sub, requirements:

	- (done) Reconnect on errors
	- (done) Full duplex protocol
	- (later) Keepalive
	- (later) Authentication via TLS
*/

func main() {
	// serverPort := flag.Int("port", 0, "server port (always required)")
	serverPort := config.PUBSUB_PORT

	subTopic := flag.String("sub-topic", "", "sub-topic")

	pubTopic := flag.String("pub-topic", "", "pub-topic")
	pubMessage := flag.String("pub-msg", "", "pub-msg")

	flag.Parse()

	if *pubTopic != "" {
		testPublish(serverPort, *pubTopic, *pubMessage)
	} else if *subTopic != "" {
		startSubscriber(serverPort, *subTopic)
	} else if serverPort != 0 {
		pubSubServer := server.NewESPubSubServer("0.0.0.0:" + strconv.Itoa(serverPort))

		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		log.Println(<-ch)

		pubSubServer.Close()
	} else {
		log.Fatalf("Usage: -port=...")
	}
}
