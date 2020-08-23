// ReaderWriter + SnapshotStore client for EventHorizon's HTTP API
package ehserverclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/gokit/ezhttp"
)

type ReaderWriterSnapshotStore interface {
	eh.ReaderWriter
	eh.SnapshotStore
}

type serverClient struct {
	authToken string // export EVENTHORIZON="http://:<apikey>@localhost"
	baseUrl   string
}

func New(urlSerialized string) (ReaderWriterSnapshotStore, error) {
	urlParsed, err := url.Parse(urlSerialized)
	if err != nil {
		return nil, err
	}

	if urlParsed.User == nil {
		return nil, errors.New("auth token not set")
	}

	if urlParsed.User.Username() != "" {
		return nil, fmt.Errorf("specifying username is not supported: %s", urlSerialized)
	}

	authToken, _ := urlParsed.User.Password()
	if authToken == "" {
		return nil, errors.New("auth token not set")
	}

	// remove user info from URL
	urlParsed.User = nil

	return &serverClient{
		authToken: authToken,
		baseUrl:   urlParsed.String(),
	}, nil
}

func (s *serverClient) Read(
	ctx context.Context,
	after eh.Cursor,
) (*eh.ReadResult, error) {
	res := &eh.ReadResult{}
	if _, err := ezhttp.Get(
		ctx,
		s.baseUrl+"/read?after="+url.QueryEscape(after.Serialize()),
		ezhttp.AuthBearer(s.authToken),
		ezhttp.RespondsJson(res, false),
	); err != nil {
		return nil, fmt.Errorf("Read: %w", err)
	}

	return res, nil
}

func (s *serverClient) Append(
	ctx context.Context,
	stream eh.StreamName,
	events []string,
) (*eh.AppendResult, error) {
	res := &eh.AppendResult{}
	if _, err := ezhttp.Post(
		ctx,
		s.baseUrl+"/append?stream="+url.QueryEscape(stream.String()),
		ezhttp.AuthBearer(s.authToken),
		ezhttp.SendJson(events),
		ezhttp.RespondsJson(res, false),
	); err != nil {
		return nil, fmt.Errorf("Append: %w", err)
	}

	return res, nil
}

func (s *serverClient) AppendAfter(
	ctx context.Context,
	after eh.Cursor,
	events []string,
) (*eh.AppendResult, error) {
	res := &eh.AppendResult{}
	if _, err := ezhttp.Post(
		ctx,
		s.baseUrl+"/append-after?after="+url.QueryEscape(after.Serialize()),
		ezhttp.AuthBearer(s.authToken),
		ezhttp.SendJson(events),
		ezhttp.RespondsJson(res, false),
	); err != nil {
		return nil, fmt.Errorf("AppendAfter: %w", err)
	}

	return res, nil
}

func (s *serverClient) CreateStream(
	ctx context.Context,
	stream eh.StreamName,
	initialEvents []string,
) (*eh.AppendResult, error) {
	res := &eh.AppendResult{}
	if _, err := ezhttp.Post(
		ctx,
		s.baseUrl+"/stream-create?stream="+url.QueryEscape(stream.String()),
		ezhttp.AuthBearer(s.authToken),
		ezhttp.SendJson(initialEvents),
		ezhttp.RespondsJson(res, false),
	); err != nil {
		return nil, fmt.Errorf("CreateStream: %w", err)
	}

	return res, nil
}

func (s *serverClient) ReadSnapshot(
	ctx context.Context,
	stream eh.StreamName,
	snapshotContext string,
) (*eh.Snapshot, error) {
	snap := &eh.Snapshot{}
	if _, err := ezhttp.Get(
		ctx,
		s.baseUrl+"/snapshot?stream="+url.QueryEscape(stream.String())+"&context="+url.QueryEscape(snapshotContext),
		ezhttp.AuthBearer(s.authToken),
		ezhttp.RespondsJson(snap, false),
	); err != nil {
		if is404(err) {
			return nil, os.ErrNotExist
		} else {
			return nil, fmt.Errorf("ReadSnapshot(%s, %s): %w", stream.String(), snapshotContext, err)
		}
	}

	return snap, nil
}

func (s *serverClient) WriteSnapshot(
	ctx context.Context,
	snapshot eh.Snapshot,
) error {
	if _, err := ezhttp.Put(
		ctx,
		s.baseUrl+"/snapshot",
		ezhttp.AuthBearer(s.authToken),
		ezhttp.SendJson(snapshot),
	); err != nil {
		return fmt.Errorf(
			"WriteSnapshot(%s, %s): %w",
			snapshot.Cursor.Stream().String(),
			snapshot.Context,
			err)
	}

	return nil
}

func (s *serverClient) DeleteSnapshot(
	ctx context.Context,
	stream eh.StreamName,
	snapshotContext string,
) error {
	if _, err := ezhttp.Del(
		ctx,
		s.baseUrl+"/snapshot?stream="+url.QueryEscape(stream.String())+"&context="+url.QueryEscape(snapshotContext),
		ezhttp.AuthBearer(s.authToken),
	); err != nil {
		if is404(err) {
			return os.ErrNotExist
		} else {
			return fmt.Errorf("DeleteSnapshot: %w", err)
		}
	}

	return nil
}

func is404(err error) bool {
	if err, isStatusError := err.(*ezhttp.ResponseStatusError); isStatusError && err.StatusCode() == http.StatusNotFound {
		return true
	} else {
		return false
	}
}
