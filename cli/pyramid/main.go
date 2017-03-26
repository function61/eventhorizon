package main

import (
	"github.com/function61/pyramid/config/configfactory"
	"github.com/function61/pyramid/pubsub/client"
	"github.com/function61/pyramid/pubsub/server"
	"github.com/function61/pyramid/pusher"
	"github.com/function61/pyramid/pusher/transport"
	"github.com/function61/pyramid/util/clicommon"
	"github.com/function61/pyramid/writer"
	wtypes "github.com/function61/pyramid/writer/types"
	"github.com/function61/pyramid/writer/writerclient"
	"github.com/function61/pyramid/writer/writerhttp"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
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

	if err := clicommon.CheckForS3AccessKeys(); err != nil {
		log.Fatalf("main: %s", err.Error())
	}

	confCtx := configfactory.Build()

	// start pub/sub server
	pubSubServer := server.New(confCtx)

	esServer := writer.New(confCtx)

	httpCloser := make(chan bool)
	httpCloserDone := make(chan bool)
	writerhttp.HttpServe(esServer, httpCloser, httpCloserDone, confCtx)

	log.Printf("main: waiting for stop signal")

	log.Println(clicommon.WaitForInterrupt())

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

	wclient := writerclient.New(configfactory.Build())

	req := &wtypes.AppendToStreamRequest{
		Stream: args[0],
		Lines:  []string{args[1]},
	}

	_, err := wclient.Append(req)
	return err
}

func streamSubscribe(args []string) error {
	if len(args) != 2 {
		return usage("<Stream> <SubscriptionId>")
	}

	wclient := writerclient.New(configfactory.Build())

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

	wclient := writerclient.New(configfactory.Build())

	req := &wtypes.CreateStreamRequest{
		Name: args[0],
	}

	_, err := wclient.CreateStream(req)
	return err
}

func streamUnsubscribe(args []string) error {
	if len(args) != 2 {
		return usage("<Stream> <SubscriptionId>")
	}

	wclient := writerclient.New(configfactory.Build())

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

	pubSubClient := client.New(configfactory.Build())
	pubSubClient.Subscribe(args[0])

	go func() {
		for {
			msg, more := <-pubSubClient.Notifications
			if !more {
				break
			}

			log.Printf("Recv: %v", msg)
		}
	}()

	clicommon.WaitForInterrupt()

	pubSubClient.Close()

	return nil
}

// Imports events into a stream from a file with one line per event.
func streamAppendFromFile(args []string) error {
	if len(args) != 2 {
		return usage("<Stream> <FilePath>")
	}

	wclient := writerclient.New(configfactory.Build())

	started := time.Now()

	linesRead := readLinebatchesFromFile(args[1], func(batch []string) error {
		appendRequest := &wtypes.AppendToStreamRequest{
			Stream: args[0],
			Lines:  batch,
		}

		log.Printf("Appending %d lines", len(batch))

		_, err := wclient.Append(appendRequest)
		return err
	})

	log.Printf("Done. Imported %d lines in %s.", linesRead, time.Since(started))

	return nil
}

func pusher_(args []string) error {
	if len(args) != 2 {
		// try "http://127.0.0.1:8080/_pyramid_push"
		return usage("<StopOnStdinEof> <Target>")
	}

	if err := clicommon.CheckForS3AccessKeys(); err != nil {
		log.Fatalf("main: %s", err.Error())
	}

	httpTarget := transport.NewHttpJsonTransport(args[1])

	psh := pusher.New(configfactory.Build(), httpTarget)
	go psh.Run()

	stdinEofOrInterrupt := make(chan bool)

	go func() {
		if args[0] != "y" {
			return
		}

		// this is for when Pusher is started as a child process of the endpoint
		// application. it makes zero sense for the child to exist after the parent
		// dies, so our parent gives us STDIN pipe which never gets written into
		// but is kept open to detect EOF (parent either exited gracefully or
		// uncleanly - doesn't matter because the OS cleans up the FDs).
		// http://stackoverflow.com/a/42924532
		clicommon.WaitForStdinEof()

		log.Printf("pusher: EOF encountered")

		stdinEofOrInterrupt <- true
	}()
	go func() {
		log.Println(clicommon.WaitForInterrupt())

		stdinEofOrInterrupt <- true
	}()

	<-stdinEofOrInterrupt

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

	wclient := writerclient.New(configfactory.Build())

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
		"writer-bootstrap":      writerBootstrap,
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
