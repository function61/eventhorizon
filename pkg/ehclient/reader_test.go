package ehclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/function61/eventhorizon/pkg/cryptosvc"
	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient/ehclienttest"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/gokit/crypto/envelopeenc"
	"github.com/function61/gokit/sync/syncutil"
	"github.com/function61/gokit/testing/assert"
)

func TestReaderReadIntoProjection(t *testing.T) {
	client, ctx := newTestingClient()

	stream := client.CreateStreamT(t, "/chatrooms/offtopic")

	chatRoom := newChatRoomProjection(stream)

	t0 := time.Date(2020, 2, 12, 13, 45, 0, 0, time.UTC)

	client.AppendT(t, stream, NewChatMessage(1, "Testing first message", ehevent.Meta(t0, "joonas")))
	client.AppendT(t, stream, NewChatMessage(2, "Is anybody listening?", ehevent.Meta(t0.Add(2*time.Minute), "joonas")))

	reader := NewReader(chatRoom, client.SystemClient, nil)

	// transactionally pumps events from event log into the projection
	assert.Ok(t, reader.LoadUntilRealtime(ctx))

	assert.EqualString(t, chatRoom.PrintChatLog(), `
13:45:00 joonas: Testing first message
13:47:00 joonas: Is anybody listening?`)

	client.AppendT(t, stream, NewChatMessage(3, "So lonely :(", ehevent.Meta(t0.Add(47*time.Minute), "joonas")))

	assert.Ok(t, reader.LoadUntilRealtime(ctx))

	assert.EqualString(t, chatRoom.PrintChatLog(), `
13:45:00 joonas: Testing first message
13:47:00 joonas: Is anybody listening?
14:32:00 joonas: So lonely :(`)
}

func TestTransactWriteFailsEachTry(t *testing.T) {
	client, _ := newTestingClient()

	stream := client.CreateStreamT(t, "/chatrooms/offtopic")

	client.AppendT(t, stream, NewChatMessage(1, "Testing first message", ehevent.Meta(t0, "joonas")))

	reader := NewReader(newChatRoomProjection(stream), client.SystemClient, nil)

	tryNumber := 0

	err := reader.TransactWrite(context.Background(), func() error {
		tryNumber++

		return eh.NewErrOptimisticLockingFailed(fmt.Errorf("try %d", tryNumber))
	})

	assert.EqualString(t, err.Error(), "maxTries failed (4): try 4")
}

func TestTransactWriteSucceedsOnThirdTry(t *testing.T) {
	client, ctx := newTestingClient()

	stream := client.CreateStreamT(t, "/chatrooms/offtopic")

	client.AppendT(t, stream, NewChatMessage(1, "Testing first message", ehevent.Meta(t0, "joonas")))

	chatRoom := newChatRoomProjection(stream)
	chatRoom.includeSequenceNumbers = true

	logBuf := &bytes.Buffer{}

	reader := NewReader(chatRoom, client.SystemClient, log.New(logBuf, "", 0))

	assert.Ok(t, reader.LoadUntilRealtime(ctx))

	tryNumber := 0

	assert.Ok(t, reader.TransactWrite(ctx, func() error {
		tryNumber++

		// inject a new chat message (that our read model - chatRoom - doesn't yet know
		// about) so our AppendAfter() won't succeed on this try
		if tryNumber == 1 || tryNumber == 2 {
			client.AppendT(t, stream, NewChatMessage(len(chatRoom.chatLog)+1, "Conflict causing message", ehevent.Meta(t0, "memelord")))
		}

		msg := NewChatMessage(len(chatRoom.chatLog)+1, "My last message", ehevent.Meta(t0, "joonas"))

		return client.AppendAfter(ctx, chatRoom.cur, msg)
	}))

	assert.EqualString(t, "\n"+logBuf.String(), `
[INFO] no initial snapshot for /chatrooms/offtopic
[DEBUG] reached realtime: /chatrooms/offtopic@0
[INFO] ErrOptimisticLockingFailed, try 1: conflict: /chatrooms/offtopic afterRequested=1 afterActual=2
[DEBUG] reached realtime: /chatrooms/offtopic@1
[INFO] ErrOptimisticLockingFailed, try 2: conflict: /chatrooms/offtopic afterRequested=2 afterActual=3
[DEBUG] reached realtime: /chatrooms/offtopic@2
`)

	assert.Ok(t, reader.LoadUntilRealtime(ctx))

	assert.EqualString(t, chatRoom.PrintChatLog(), `
13:45:00 1 joonas: Testing first message
13:45:00 2 memelord: Conflict causing message
13:45:00 3 memelord: Conflict causing message
13:45:00 4 joonas: My last message`)
}

func newChatRoomProjection(stream eh.StreamName) *chatRoomProjection {
	return &chatRoomProjection{
		cur:     stream.Beginning(),
		chatLog: []string{},
	}
}

type chatRoomProjection struct {
	cur                    eh.Cursor
	chatLog                []string
	includeSequenceNumbers bool
}

func (d *chatRoomProjection) PrintChatLog() string {
	return "\n" + strings.Join(d.chatLog, "\n")
}

func (d *chatRoomProjection) GetEventTypes() []LogDataKindDeserializer {
	return EncryptedDataDeserializer(testingEventTypes)
}

func (d *chatRoomProjection) InstallSnapshot(snap *eh.Snapshot) error {
	d.cur = snap.Cursor
	return json.Unmarshal(snap.Data, &d.chatLog)
}

func (d *chatRoomProjection) Snapshot() (*eh.Snapshot, error) {
	data, err := json.MarshalIndent(&d.chatLog, "", "  ")
	if err != nil {
		return nil, err
	}

	return eh.NewSnapshot(d.cur, data, d.SnapshotContextAndVersion()), nil
}

func (d *chatRoomProjection) SnapshotContextAndVersion() string {
	return "chat:v1"
}

func (d *chatRoomProjection) ProcessEvents(ctx context.Context, handle EventProcessorHandler) error {
	return handle(
		d.cur,
		func(e ehevent.Event) error { return d.processEvent(e) },
		func(commitCursor eh.Cursor) error {
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
			e.Meta().Time().Format("15:04:05"),
			maybeSequenceNumber,
			e.Meta().UserIdOrEmptyIfSystem(),
			e.Message)

		d.chatLog = append(d.chatLog, msgDisplay)
	default:
		return UnsupportedEventTypeErr(ev)
	}

	return nil
}

var testingEventTypes = ehevent.Types{
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

// contains test helpers for action wrappers with less boilerplate
type SystemClientTesting struct {
	*SystemClient

	TestSnapshotStore *ehclienttest.SnapshotStore
}

func (e *SystemClientTesting) AppendT(t *testing.T, stream eh.StreamName, events ...ehevent.Event) {
	assert.Ok(t, e.Append(context.Background(), stream, events...))
}

func (s *SystemClientTesting) CreateStreamT(t *testing.T, streamName string) eh.StreamName {
	stream, err := eh.DeserializeStreamName(streamName)
	assert.Ok(t, err)

	_, err = s.CreateStream(context.Background(), stream, nil)
	assert.Ok(t, err)

	return stream
}

func (s *SystemClientTesting) DekForT(t *testing.T, stream eh.StreamName) []byte {
	dekEnvelope, err := s.resolveDekEnvelope(context.Background(), stream)
	assert.Ok(t, err)
	dek, err := s.cryptoSvc.DecryptEnvelope(context.Background(), *dekEnvelope)
	assert.Ok(t, err)
	return dek
}

func newTestingClient() (*SystemClientTesting, context.Context) {
	eventLog := ehclienttest.NewEventLog()
	snapshotStore := ehclienttest.NewSnapshotStore()

	return &SystemClientTesting{
		SystemClient: &SystemClient{
			EventLog:      eventLog,
			SnapshotStore: snapshotStore,
			resolveDekEnvelope: func(_ context.Context, stream eh.StreamName) (*envelopeenc.Envelope, error) {
				dekEnvelope := eventLog.ResolveDekEnvelope(stream)
				if dekEnvelope == nil {
					return nil, fmt.Errorf("Failed to resolve DEK envelope for %s", stream.String())
				}

				return dekEnvelope, nil
			},
			cryptoSvc: cryptosvc.New(nil),

			deksCache:         map[string][]byte{},
			deksCacheStreamMu: syncutil.NewMutexMap(),
		},
		TestSnapshotStore: snapshotStore,
	}, context.Background()
}
