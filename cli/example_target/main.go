package main

import (
	"github.com/function61/pyramid/cli"
	"github.com/function61/pyramid/cli/example_target/target"
)

func main() {
	target.Serve()

	cli.WaitForInterrupt()
}
