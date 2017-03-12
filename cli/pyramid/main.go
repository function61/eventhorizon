package main

import (
	"github.com/function61/pyramid/cli"
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/pubsub/client"
	"github.com/function61/pyramid/pubsub/server"
	"github.com/function61/pyramid/pusher"
	"github.com/function61/pyramid/writer"
	wtypes "github.com/function61/pyramid/writer/types"
	"github.com/function61/pyramid/writer/writerclient"
	"github.com/function61/pyramid/writer/writerhttp"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

func banner() {
	log.Println("       .")
	log.Println("      /=\\\\       PyramidDB")
	log.Println("     /===\\ \\     function61.com")
	log.Println("    /=====\\  \\")
	log.Println("   /=======\\  /")
	log.Println("  /=========\\/")
}

func writer_(args []string) error {
	if len(args) != 0 {
		return usage("(no args)")
	}

	banner()

	if err := cli.CheckForS3AccessKeys(); err != nil {
		log.Fatalf("main: %s", err.Error())
	}

	// start pub/sub server
	pubSubServer := server.NewESPubSubServer("0.0.0.0:" + strconv.Itoa(config.PUBSUB_PORT))

	esServer := writer.NewEventstoreWriter()

	httpCloser := make(chan bool)
	httpCloserDone := make(chan bool)
	writerhttp.HttpServe(esServer, httpCloser, httpCloserDone)

	log.Printf("main: waiting for stop signal")

	log.Println(cli.WaitForInterrupt())

	// stop serving HTTP

	httpCloser <- true
	<-httpCloserDone

	// stop the main writer server

	esServer.Close()

	// stop pub/sub
	pubSubServer.Close()

	return nil
}

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

	pubSubClient := client.New("127.0.0.1:" + strconv.Itoa(config.PUBSUB_PORT))
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

	psh := pusher.New(exampleReceiverTarget)
	go psh.Run()

	log.Println(cli.WaitForInterrupt())

	psh.Close()

	return nil
}

func streamLiveRead(args []string) error {
	if len(args) != 2 {
		return usage("<Cursor> <LinesToRead>")
	}

	maxLinesToRead, atoiErr := strconv.Atoi(args[1])
	if atoiErr != nil {
		return atoiErr
	}

	wclient := writerclient.NewClient()

	req := &wtypes.LiveReadInput{
		Cursor:         args[0],
		MaxLinesToRead: maxLinesToRead,
	}

	reader, _, err := wclient.LiveRead(req)
	if err != nil {
		return err
	}

	if _, err := io.Copy(os.Stdout, reader); err != nil {
		return err
	}

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
		"stream-liveread":       streamLiveRead,
		"pubsub-subscribe":      pubsubSubscribe,
		"pusher":                pusher_,
		"writer":                writer_,
	}

	if len(os.Args) < 2 {
		banner()
		log.Fatalf(
			"Usage: %s <subcommand>\n\nSubcommands: \n  %s",
			os.Args[0],
			strings.Join(stringKeyedMapToStringSlice(mapping), "\n  "))
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
