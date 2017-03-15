package main

import (
	"github.com/function61/pyramid/cli"
	"github.com/function61/pyramid/cli/example_target/target"
)

func main() {
	receiver := target.NewReceiver()
	receiver.Run()

	cli.WaitForInterrupt()
}
