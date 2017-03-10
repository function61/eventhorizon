package main

import (
	wtypes "github.com/function61/pyramid/writer/types"
	"github.com/function61/pyramid/writer/writerclient"
	"log"
	"os"
	"strings"
)

func append_(args []string) error {
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

func subscribe(args []string) error {
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

func create(args []string) error {
	if len(args) != 1 {
		return usage("<Stream>")
	}

	wclient := writerclient.NewClient()

	req := &wtypes.CreateStreamRequest{
		Name: args[0],
	}

	return wclient.CreateStream(req)
}

func unsubscribe(args []string) error {
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

// Imports events into a stream from a file with one line per event.

func importfromfile(args []string) error {
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

// just a dispatcher to the subcommands
func main() {
	mapping := map[string]func([]string) error{
		"create":         create,
		"append":         append_,
		"importfromfile": importfromfile,
		"subscribe":      subscribe,
		"unsubscribe":    unsubscribe,
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
