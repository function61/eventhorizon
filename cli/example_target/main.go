package main

import (
	"github.com/function61/pyramid/cli"
	"github.com/function61/pyramid/cli/example_target/target"
)

func main() {
	receiver := target.NewTarget()
	receiver.Run()

	cli.WaitForInterrupt()
}
