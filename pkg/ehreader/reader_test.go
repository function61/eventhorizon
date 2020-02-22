package ehreader

import (
	"context"
	"fmt"
	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/ehreader/ehreadertest"
	"github.com/function61/gokit/assert"
	"strings"
	"testing"
	"time"
)

func TestReaderReadIntoProjection(t *testing.T) {
	stream := "/chatrooms/offtopic"

	chatRoom := &chatRoomProjection{
		cur:     ehclient.Beginning(stream),
		chatLog: []string{},
	}

	t0 := time.Date(2020, 2, 12, 13, 45, 0, 0, time.UTC)

	eventLog := ehreadertest.NewEventLog()
	eventLog.AppendE(stream, NewChatMessage("1", "Testing first message", ehevent.Meta(t0, "joonas")))
	eventLog.AppendE(stream, NewChatMessage("2", "Is anybody listening?", ehevent.Meta(t0.Add(2*time.Minute), "joonas")))

	reader := New(chatRoom, eventLog)

	// transactionally pumps events from event log into the projection
	assert.Ok(t, reader.LoadUntilRealtime(context.Background()))

	assert.EqualString(t, chatRoom.PrintChatLog(), `
13:45:00 joonas: Testing first message
13:47:00 joonas: Is anybody listening?`)

	eventLog.AppendE(stream, NewChatMessage("3", "So lonely :(", ehevent.Meta(t0.Add(47*time.Minute), "joonas")))

	assert.Ok(t, reader.LoadUntilRealtime(context.Background()))

	assert.EqualString(t, chatRoom.PrintChatLog(), `
13:45:00 joonas: Testing first message
13:47:00 joonas: Is anybody listening?
14:32:00 joonas: So lonely :(`)
}

type chatRoomProjection struct {
	cur     ehclient.Cursor
	chatLog []string
}

func (d *chatRoomProjection) PrintChatLog() string {
	return "\n" + strings.Join(d.chatLog, "\n")
}

func (d *chatRoomProjection) GetEventTypes() ehevent.Allocators {
	return testingEventTypes
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
		msgDisplay := fmt.Sprintf(
			"%s %s: %s",
			e.Meta().Timestamp.Format("15:04:05"),
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
	Id      string
	Message string
}

func (e *ChatMessage) MetaType() string         { return "chat.Message" }
func (e *ChatMessage) Meta() *ehevent.EventMeta { return &e.meta }

func NewChatMessage(
	id string,
	message string,
	meta ehevent.EventMeta,
) *ChatMessage {
	return &ChatMessage{
		meta:    meta,
		Id:      id,
		Message: message,
	}
}
