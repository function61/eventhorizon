package ehreader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/ehreader/ehreadertest"
	"github.com/function61/gokit/assert"
	"log"
	"strings"
	"testing"
	"time"
)

func TestReaderReadIntoProjection(t *testing.T) {
	stream := "/chatrooms/offtopic"

	chatRoom := newChatRoomProjection(stream)

	t0 := time.Date(2020, 2, 12, 13, 45, 0, 0, time.UTC)

	eventLog := ehreadertest.NewEventLog()
	eventLog.AppendE(stream, NewChatMessage(1, "Testing first message", ehevent.Meta(t0, "joonas")))
	eventLog.AppendE(stream, NewChatMessage(2, "Is anybody listening?", ehevent.Meta(t0.Add(2*time.Minute), "joonas")))

	reader := New(chatRoom, eventLog, nil)

	// transactionally pumps events from event log into the projection
	assert.Ok(t, reader.LoadUntilRealtime(context.Background()))

	assert.EqualString(t, chatRoom.PrintChatLog(), `
13:45:00 joonas: Testing first message
13:47:00 joonas: Is anybody listening?`)

	eventLog.AppendE(stream, NewChatMessage(3, "So lonely :(", ehevent.Meta(t0.Add(47*time.Minute), "joonas")))

	assert.Ok(t, reader.LoadUntilRealtime(context.Background()))

	assert.EqualString(t, chatRoom.PrintChatLog(), `
13:45:00 joonas: Testing first message
13:47:00 joonas: Is anybody listening?
14:32:00 joonas: So lonely :(`)
}

func TestTransactWriteFailsEachTry(t *testing.T) {
	stream := "/chatrooms/offtopic"

	eventLog := ehreadertest.NewEventLog()
	eventLog.AppendE(stream, NewChatMessage(1, "Testing first message", ehevent.Meta(t0, "joonas")))

	reader := New(newChatRoomProjection(stream), eventLog, nil)

	tryNumber := 0

	err := reader.TransactWrite(context.Background(), func() error {
		tryNumber++

		return ehclient.NewErrOptimisticLockingFailed(fmt.Errorf("try %d", tryNumber))
	})

	assert.EqualString(t, err.Error(), "maxTries failed (4): try 4")
}

func TestTransactWriteSucceedsOnThirdTry(t *testing.T) {
	ctx := context.Background()

	stream := "/chatrooms/offtopic"

	eventLog := ehreadertest.NewEventLog()
	eventLog.AppendE(stream, NewChatMessage(1, "Testing first message", ehevent.Meta(t0, "joonas")))

	chatRoom := newChatRoomProjection(stream)
	chatRoom.includeSequenceNumbers = true

	logBuf := &bytes.Buffer{}

	reader := New(chatRoom, eventLog, log.New(logBuf, "", 0))

	assert.Ok(t, reader.LoadUntilRealtime(ctx))

	tryNumber := 0

	assert.Ok(t, reader.TransactWrite(ctx, func() error {
		tryNumber++

		// inject new chat message so our Append() won't succeed on this try
		if tryNumber == 1 || tryNumber == 2 {
			eventLog.AppendE(stream, NewChatMessage(len(chatRoom.chatLog)+1, "Conflict causing message", ehevent.Meta(t0, "memelord")))
		}

		msg := NewChatMessage(len(chatRoom.chatLog)+1, "My last message", ehevent.Meta(t0, "joonas"))

		_, err := eventLog.AppendAfter(ctx, chatRoom.cur, []string{ehevent.Serialize(msg)})
		return err
	}))

	assert.EqualString(t, logBuf.String(), `[DEBUG] ErrOptimisticLockingFailed, try 1: conflict: afterRequested=/chatrooms/offtopic@1 afterActual=/chatrooms/offtopic@2
[DEBUG] ErrOptimisticLockingFailed, try 2: conflict: afterRequested=/chatrooms/offtopic@2 afterActual=/chatrooms/offtopic@3
`)

	assert.Ok(t, reader.LoadUntilRealtime(ctx))

	assert.EqualString(t, chatRoom.PrintChatLog(), `
13:45:00 1 joonas: Testing first message
13:45:00 2 memelord: Conflict causing message
13:45:00 3 memelord: Conflict causing message
13:45:00 4 joonas: My last message`)
}

func newChatRoomProjection(stream string) *chatRoomProjection {
	return &chatRoomProjection{
		cur:     ehclient.Beginning(stream),
		chatLog: []string{},
	}
}

type chatRoomProjection struct {
	cur                    ehclient.Cursor
	chatLog                []string
	includeSequenceNumbers bool
}

func (d *chatRoomProjection) PrintChatLog() string {
	return "\n" + strings.Join(d.chatLog, "\n")
}

func (d *chatRoomProjection) GetEventTypes() ehevent.Allocators {
	return testingEventTypes
}

func (d *chatRoomProjection) InstallSnapshot(snap *Snapshot) error {
	d.cur = snap.Cursor
	return json.Unmarshal(snap.Data, &d.chatLog)
}

func (d *chatRoomProjection) Snapshot() (*Snapshot, error) {
	data, err := json.MarshalIndent(&d.chatLog, "", "  ")
	if err != nil {
		return nil, err
	}

	return NewSnapshot(d.cur, data), nil
}

func (d *chatRoomProjection) ProcessEvents(ctx context.Context, handle EventProcessorHandler) error {
	return handle(
		d.cur,
		func(e ehevent.Event) error { return d.processEvent(e) },
		func(commitCursor ehclient.Cursor) error {
			d.cur = commitCursor
			return nil
		})
}

func (d *chatRoomProjection) processEvent(ev ehevent.Event) error {
	switch e := ev.(type) {
	case *ChatMessage:
		maybeSequenceNumber := ""
		if d.includeSequenceNumbers {
			maybeSequenceNumber = fmt.Sprintf(" %d", e.Id)
		}

		msgDisplay := fmt.Sprintf(
			"%s%s %s: %s",
			e.Meta().Timestamp.Format("15:04:05"),
			maybeSequenceNumber,
			e.Meta().UserId,
			e.Message)

		d.chatLog = append(d.chatLog, msgDisplay)
	default:
		return UnsupportedEventTypeErr(ev)
	}

	return nil
}

var testingEventTypes = ehevent.Allocators{
	"chat.Message": func() ehevent.Event { return &ChatMessage{} },
}

type ChatMessage struct {
	meta    ehevent.EventMeta
	Id      int
	Message string
}

func (e *ChatMessage) MetaType() string         { return "chat.Message" }
func (e *ChatMessage) Meta() *ehevent.EventMeta { return &e.meta }

func NewChatMessage(
	id int,
	message string,
	meta ehevent.EventMeta,
) *ChatMessage {
	return &ChatMessage{
		meta:    meta,
		Id:      id,
		Message: message,
	}
}
