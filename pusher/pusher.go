package pusher

import (
	"github.com/function61/eventhorizon/cursor"
	"github.com/function61/eventhorizon/reader"
	"log"
)

type Pusher struct {
	receiver *Receiver
	reader   *reader.EventstoreReader
}

func NewPusher() *Pusher {
	r := NewReceiver()

	return &Pusher{
		receiver: r,
		reader:   reader.NewEventstoreReader(),
	}
}

func (p *Pusher) Run() {
	stream := "/tenants/foo"

	offset, err := p.receiver.QueryOffset(stream)
	if err != nil {
		log.Printf("Pusher: error querying receiver offset %s", err.Error())
		return
	}

	log.Printf("Pusher: receiver's starting offset for %s is %s", stream, offset)

	cur := cursor.CursorFromserializedMust(offset)

	p.pushOne(cur)
}

func (p *Pusher) pushOne(cur *cursor.Cursor) {
	readReq := reader.NewReadOptions()
	readReq.Cursor = cur

	readResult, err := p.reader.Read(readReq)
	if err != nil {
		panic(err)
	}

	pushResult, err := p.receiver.PushReadResult(readResult)
	if err != nil {
		panic(err)
	}

	log.Printf("Pusher: receiver ACKed until %s", pushResult.AcceptedOffset)
}
