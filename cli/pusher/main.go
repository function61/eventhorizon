package main

import (
	"github.com/function61/eventhorizon/pusher"
)

func main() {
	psh := pusher.NewPusher()
	psh.Run()
}
