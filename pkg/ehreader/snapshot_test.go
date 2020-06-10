package ehreader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/ehreader/ehreadertest"
	"github.com/function61/gokit/assert"
)

var (
	t0 = time.Date(2020, 2, 12, 13, 45, 0, 0, time.UTC)
)

func TestSnapshot(t *testing.T) {
	ctx := context.Background()

	css := &countingSnapshotStore{NewInMemSnapshotStore(), 0}

	initialSnap := NewSnapshot(ehclient.At("/chatrooms/offtopic", 1), []byte(`[
		"12:00:01 joonas: First msg from snapshot",
		"12:00:02 joonas: Second msg from snapshot"
	]`))

	assert.Ok(t, css.StoreSnapshot(ctx, *initialSnap))

	assert.Assert(t, css.storeCalls == 1)

	stream := "/chatrooms/offtopic"

	eventLog := ehreadertest.NewEventLog()
	// as a hack we'll put different content on the event log than the snapshot version of
	// the events, so it's easy for us to distinguish that our logic started looking at log
	// from 3rd event onwards
	eventLog.AppendE(stream, NewChatMessage(1, "This message not actually processed 1", ehevent.Meta(t0, "joonas")))
	eventLog.AppendE(stream, NewChatMessage(2, "This message not actually processed 2", ehevent.Meta(t0.Add(2*time.Minute), "joonas")))

	// this message is not contained in the snapshot
	eventLog.AppendE(stream, NewChatMessage(3, "Third msg from log", ehevent.Meta(t0.Add(3*time.Minute), "joonas")))

	chatRoom := newChatRoomProjection(stream)

	reader := NewWithSnapshots(chatRoom, eventLog, css, nil)

	assert.Ok(t, reader.LoadUntilRealtime(ctx))

	assert.Assert(t, css.storeCalls == 2)

	assert.EqualString(t, chatRoom.PrintChatLog(), `
12:00:01 joonas: First msg from snapshot
12:00:02 joonas: Second msg from snapshot
13:48:00 joonas: Third msg from log`)

	snap, err := css.LoadSnapshot(ctx, chatRoom.cur)
	assert.Ok(t, err)

	assert.EqualString(t, string(snap.Data), `[
  "12:00:01 joonas: First msg from snapshot",
  "12:00:02 joonas: Second msg from snapshot",
  "13:48:00 joonas: Third msg from log"
]`)

	// non-advancing reads from log should not try to store snapshots
	assert.Ok(t, reader.LoadUntilRealtime(ctx))
	assert.Assert(t, css.storeCalls == 2)
}

func TestDontMindSnapshotNotFoundOrStoreFails(t *testing.T) {
	stream := "/chatrooms/offtopic"

	eventLog := ehreadertest.NewEventLog()
	eventLog.AppendE(stream, NewChatMessage(1, "Hello", ehevent.Meta(t0, "joonas")))

	chatRoom := newChatRoomProjection(stream)

	logBuffer := &bytes.Buffer{}

	reader := NewWithSnapshots(chatRoom, eventLog, &storingSnapshotFails{}, log.New(logBuffer, "", 0))

	assert.Ok(t, reader.LoadUntilRealtime(context.Background()))

	assert.EqualString(t, chatRoom.PrintChatLog(), `
13:45:00 joonas: Hello`)

	assert.EqualString(t, logBuffer.String(), `[INFO] LoadSnapshot: initial snapshot not found for /chatrooms/offtopic
[ERROR] StoreSnapshot: too lazy to store /chatrooms/offtopic@0
`)
}

func TestLoadingSnapshotFails(t *testing.T) {
	stream := "/chatrooms/offtopic"

	eventLog := ehreadertest.NewEventLog()
	eventLog.AppendE(stream, NewChatMessage(1, "Hello", ehevent.Meta(t0, "joonas")))

	chatRoom := newChatRoomProjection(stream)

	logBuffer := &bytes.Buffer{}

	reader := NewWithSnapshots(chatRoom, eventLog, &loadingSnapshotFails{}, log.New(logBuffer, "", 0))

	assert.Ok(t, reader.LoadUntilRealtime(context.Background()))

	assert.EqualString(t, chatRoom.PrintChatLog(), `
13:45:00 joonas: Hello`)

	assert.EqualString(t, logBuffer.String(), "[ERROR] LoadSnapshot: transport error\n")
}

type countingSnapshotStore struct {
	proxied    SnapshotStore
	storeCalls int
}

var _ = (SnapshotStore)(&countingSnapshotStore{})

func (c *countingSnapshotStore) StoreSnapshot(ctx context.Context, snap Snapshot) error {
	c.storeCalls++
	return c.proxied.StoreSnapshot(ctx, snap)
}

func (c *countingSnapshotStore) LoadSnapshot(ctx context.Context, cur ehclient.Cursor) (*Snapshot, error) {
	return c.proxied.LoadSnapshot(ctx, cur)
}

type storingSnapshotFails struct{}

func (l *storingSnapshotFails) StoreSnapshot(_ context.Context, snap Snapshot) error {
	return fmt.Errorf("too lazy to store %s", snap.Cursor.Serialize())
}

func (l *storingSnapshotFails) LoadSnapshot(_ context.Context, _ ehclient.Cursor) (*Snapshot, error) {
	// not found, not an error per se
	return nil, os.ErrNotExist
}

type loadingSnapshotFails struct{}

func (l *loadingSnapshotFails) StoreSnapshot(_ context.Context, snap Snapshot) error {
	return fmt.Errorf("too lazy to store %s", snap.Cursor.Serialize())
}

func (l *loadingSnapshotFails) LoadSnapshot(_ context.Context, _ ehclient.Cursor) (*Snapshot, error) {
	return nil, errors.New("transport error")
}
