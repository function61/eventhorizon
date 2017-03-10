package main

import (
	"github.com/function61/pyramid/cli"
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/pubsub/client"
	"github.com/function61/pyramid/pusher"
	wtypes "github.com/function61/pyramid/writer/types"
	"github.com/function61/pyramid/writer/writerclient"
	"log"
	"os"
	"strconv"
	"strings"
)

func streamAppend(args []string) error {
	if len(args) != 2 {
		return usage("<Stream> <Line>")
	}

	wclient := writerclient.NewClient()

	req := &wtypes.AppendToStreamRequest{
		Stream: args[0],
		Lines:  []string{args[1]},
	}

	return wclient.Append(req)
}

func streamSubscribe(args []string) error {
	if len(args) != 2 {
		return usage("<Stream> <SubscriptionId>")
	}

	wclient := writerclient.NewClient()

	req := &wtypes.SubscribeToStreamRequest{
		Stream:         args[0],
		SubscriptionId: args[1],
	}

	return wclient.SubscribeToStream(req)
}

func streamCreate(args []string) error {
	if len(args) != 1 {
		return usage("<Stream>")
	}

	wclient := writerclient.NewClient()

	req := &wtypes.CreateStreamRequest{
		Name: args[0],
	}

	return wclient.CreateStream(req)
}

func streamUnsubscribe(args []string) error {
	if len(args) != 2 {
		return usage("<Stream> <SubscriptionId>")
	}

	wclient := writerclient.NewClient()

	req := &wtypes.UnsubscribeFromStreamRequest{
		Stream:         args[0],
		SubscriptionId: args[1],
	}

	return wclient.UnsubscribeFromStream(req)
}

func pubsubSubscribe(args []string) error {
	if len(args) != 1 {
		return usage("<Topic>")
	}

	pubSubClient := client.NewPubSubClient("127.0.0.1:" + strconv.Itoa(config.PUBSUB_PORT))
	pubSubClient.Subscribe(args[0])

	for {
		msg := <-pubSubClient.Notifications

		log.Printf("Recv: %v", msg)
	}

	// TODO: no graceful quit mechanism
	pubSubClient.Close()

	return nil
}

// Imports events into a stream from a file with one line per event.
func streamAppendFromFile(args []string) error {
	if len(args) != 2 {
		return usage("<Stream> <FilePath>")
	}

	wclient := writerclient.NewClient()

	linesRead := readLinebatchesFromFile(args[1], func(batch []string) error {
		appendRequest := &wtypes.AppendToStreamRequest{
			Stream: args[0],
			Lines:  batch,
		}

		log.Printf("Appending %d lines", len(batch))

		return wclient.Append(appendRequest)
	})

	log.Printf("Done. Imported %d lines in aggregate.", linesRead)

	return nil
}

func pusher_(args []string) error {
	if len(args) != 1 {
		return usage("<Target>")
	}

	if err := cli.CheckForS3AccessKeys(); err != nil {
		log.Fatalf("main: %s", err.Error())
	}

	exampleReceiverTarget := NewReceiver()

	psh := pusher.NewPusher(exampleReceiverTarget)
	go psh.Run()

	log.Println(cli.WaitForInterrupt())

	psh.Close()

	return nil
}

// just a dispatcher to the subcommands
func main() {
	mapping := map[string]func([]string) error{
		"stream-create":         streamCreate,
		"stream-append":         streamAppend,
		"stream-appendfromfile": streamAppendFromFile,
		"stream-subscribe":      streamSubscribe,
		"stream-unsubscribe":    streamUnsubscribe,
		"pubsub-subscribe":      pubsubSubscribe,
		"pusher":                pusher_,
	}

	if len(os.Args) < 2 {
		log.Fatalf(
			"Usage: %s %s",
			os.Args[0],
			strings.Join(stringKeyedMapToStringSlice(mapping), "|"))
	}

	subcommand := os.Args[1]

	subcommandFn, exists := mapping[subcommand]

	if !exists {
		log.Fatalf("Unknown subcommand: %s", subcommand)
	}

	if err := subcommandFn(os.Args[2:]); err != nil {
		log.Fatalf("%s: %s", subcommand, err.Error())
	}
}
