package ehclient

import (
	"testing"
	"time"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehclient/ehclienttest"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/gokit/testing/assert"
)

var (
	t0 = time.Date(2020, 2, 12, 13, 45, 0, 0, time.UTC)
)

func TestSnapshot(t *testing.T) {
	client, ctx := newTestingClient()

	snapshotStats := func() ehclienttest.SnapshotStoreStats {
		return client.TestSnapshotStore.Stats()
	}

	stream := client.CreateStreamT(t, "/chatrooms/offtopic")

	chatRoom := newChatRoomProjection(stream)

	dek := client.DEKForT(t, stream)

	initialSnap, err := eh.NewSnapshot(stream.At(1), []byte(`[
		"12:00:01 joonas: First msg from snapshot",
		"12:00:02 joonas: Second msg from snapshot"
	]`), chatRoom.SnapshotContextAndVersion()).Encrypted(dek)
	assert.Ok(t, err)

	beforeInitialSnapWrite := snapshotStats()

	assert.Assert(t, beforeInitialSnapWrite.Diff(snapshotStats()).WriteOps == 0)

	assert.Ok(t, client.SnapshotStore.WriteSnapshot(ctx, *initialSnap))

	assert.Assert(t, beforeInitialSnapWrite.Diff(snapshotStats()).WriteOps == 1)

	// as a hack we'll put different content on the event log than the snapshot version of
	// the events, so it's easy for us to distinguish that our logic started looking at log
	// from 3rd event onwards
	client.AppendT(t, stream, NewChatMessage(1, "This message not actually processed 1", ehevent.Meta(t0, "joonas")))
	client.AppendT(t, stream, NewChatMessage(2, "This message not actually processed 2", ehevent.Meta(t0.Add(2*time.Minute), "joonas")))

	// this message is not contained in the snapshot
	client.AppendT(t, stream, NewChatMessage(3, "Third msg from log", ehevent.Meta(t0.Add(3*time.Minute), "joonas")))

	reader := NewReader(chatRoom, client.SystemClient, nil)

	before3rdMsgLoad := snapshotStats()

	assert.Ok(t, reader.LoadUntilRealtime(ctx))

	assert.Assert(t, before3rdMsgLoad.Diff(snapshotStats()).WriteOps == 1)

	assert.EqualString(t, chatRoom.PrintChatLog(), `
12:00:01 joonas: First msg from snapshot
12:00:02 joonas: Second msg from snapshot
13:48:00 joonas: Third msg from log`)

	snapPersisted, err := client.SnapshotStore.ReadSnapshot(ctx, stream, chatRoom.SnapshotContextAndVersion())
	assert.Ok(t, err)

	snap, err := snapPersisted.DecryptIfRequired(func() ([]byte, error) {
		return client.DEKForT(t, stream), nil
	})
	assert.Ok(t, err)

	assert.EqualString(t, string(snap.Data), `[
  "12:00:01 joonas: First msg from snapshot",
  "12:00:02 joonas: Second msg from snapshot",
  "13:48:00 joonas: Third msg from log"
]`)

	beforeNonAdvancingRead := snapshotStats()

	// non-advancing reads from log should not try to store snapshots
	assert.Ok(t, reader.LoadUntilRealtime(ctx))

	assert.Assert(t, beforeNonAdvancingRead.Diff(snapshotStats()).WriteOps == 0)
}

/*
func TestDontMindSnapshotNotFoundOrStoreFails(t *testing.T) {
	stream := "/chatrooms/offtopic"

	eventLog := ehclienttest.NewEventLog()
	client.AppendT(t,stream, NewChatMessage(1, "Hello", ehevent.Meta(t0, "joonas")))

	chatRoom := newChatRoomProjection(stream)

	logBuffer := &bytes.Buffer{}

	reader := New(chatRoom, eventLog, &storingSnapshotFails{}, log.New(logBuffer, "", 0))

	assert.Ok(t, reader.LoadUntilRealtime(context.Background()))

	assert.EqualString(t, chatRoom.PrintChatLog(), `
13:45:00 joonas: Hello`)

	assert.EqualString(t, logBuffer.String(), `[INFO] LoadSnapshot: initial snapshot not found for /chatrooms/offtopic
[ERROR] WriteSnapshot: too lazy to store /chatrooms/offtopic@0
`)
}

func TestLoadingSnapshotFails(t *testing.T) {
	stream := "/chatrooms/offtopic"

	eventLog := ehclienttest.NewEventLog()
	client.AppendT(t,stream, NewChatMessage(1, "Hello", ehevent.Meta(t0, "joonas")))

	chatRoom := newChatRoomProjection(stream)

	logBuffer := &bytes.Buffer{}

	reader := New(chatRoom, eventLog, &loadingSnapshotFails{}, log.New(logBuffer, "", 0))

	assert.Ok(t, reader.LoadUntilRealtime(context.Background()))

	assert.EqualString(t, chatRoom.PrintChatLog(), `
13:45:00 joonas: Hello`)

	assert.EqualString(t, logBuffer.String(), "[ERROR] LoadSnapshot: transport error\n")
}
*/

/*
type storingSnapshotFails struct{}

func (l *storingSnapshotFails) WriteSnapshot(_ context.Context, snap eh.PersistedSnapshot) error {
	return fmt.Errorf("too lazy to store %s", snap.Cursor.Serialize())
}

func (l *storingSnapshotFails) ReadSnapshot(_ context.Context, _ eh.Cursor, perspective eh.SnapshotPerspective) (*eh.PersistedSnapshot, error) {
	// not found, not an error per se
	return nil, os.ErrNotExist
}

type loadingSnapshotFails struct{}

func (l *loadingSnapshotFails) WriteSnapshot(_ context.Context, snap eh.PersistedSnapshot) error {
	return fmt.Errorf("too lazy to store %s", snap.Cursor.Serialize())
}

func (l *loadingSnapshotFails) ReadSnapshot(_ context.Context, _ eh.Cursor, perspective eh.SnapshotPerspective) (*eh.PersistedSnapshot, error) {
	return nil, errors.New("transport error")
}
*/
