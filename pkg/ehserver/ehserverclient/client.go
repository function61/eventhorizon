// ReaderWriter + SnapshotStore client for EventHorizon's HTTP API
package ehserverclient

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/gokit/crypto/envelopeenc"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/net/http/ezhttp"
)

type CreateStreamInput struct {
	DEK  *envelopeenc.Envelope
	Data *eh.LogData
}

type ReaderWriterSnapshotStore interface {
	eh.ReaderWriter
	eh.SnapshotStore
}

type serverClient struct {
	authToken string
	baseUrl   string
	logl      *logex.Leveled
}

func New(
	urlRaw string,
	logger *log.Logger,
) (ReaderWriterSnapshotStore, error) {
	urlWithoutUser, authToken, err := SplitAuthTokenFromUrl(urlRaw)
	if err != nil {
		return nil, err
	}

	return &serverClient{
		authToken: authToken,
		baseUrl:   urlWithoutUser,
		logl:      logex.Levels(logger),
	}, nil
}

func (s *serverClient) Read(
	ctx context.Context,
	after eh.Cursor,
) (*eh.ReadResult, error) {
	s.logl.Debug.Printf("Read %s", after.Serialize())

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
	data eh.LogData,
) (*eh.AppendResult, error) {
	s.logl.Debug.Printf("Append")

	res := &eh.AppendResult{}
	if _, err := ezhttp.Post(
		ctx,
		s.baseUrl+"/append?stream="+url.QueryEscape(stream.String()),
		ezhttp.AuthBearer(s.authToken),
		ezhttp.SendJson(data),
		ezhttp.RespondsJson(res, false),
	); err != nil {
		return nil, fmt.Errorf("Append: %w", err)
	}

	return res, nil
}

func (s *serverClient) AppendAfter(
	ctx context.Context,
	after eh.Cursor,
	data eh.LogData,
) (*eh.AppendResult, error) {
	s.logl.Debug.Printf("AppendAfter")

	result := &eh.AppendResult{}
	if _, err := ezhttp.Post(
		ctx,
		s.baseUrl+"/append-after?after="+url.QueryEscape(after.Serialize()),
		ezhttp.AuthBearer(s.authToken),
		ezhttp.SendJson(data),
		ezhttp.RespondsJson(result, false),
	); err != nil {
		if ezhttp.ErrorIs(err, http.StatusConflict) {
			return nil, eh.NewErrOptimisticLockingFailed(err)
		} else {
			return nil, fmt.Errorf("AppendAfter: %w", err)
		}
	}

	return result, nil
}

func (s *serverClient) CreateStream(
	ctx context.Context,
	stream eh.StreamName,
	dekEnvelope envelopeenc.Envelope,
	data *eh.LogData,
) (*eh.AppendResult, error) {
	s.logl.Debug.Printf("CreateStream")

	result := &eh.AppendResult{}
	if _, err := ezhttp.Post(
		ctx,
		s.baseUrl+"/stream-create?stream="+url.QueryEscape(stream.String()),
		ezhttp.AuthBearer(s.authToken),
		ezhttp.SendJson(CreateStreamInput{
			DEK:  &dekEnvelope,
			Data: data,
		}),
		ezhttp.RespondsJson(result, false),
	); err != nil {
		return nil, fmt.Errorf("CreateStream: %w", err)
	}

	return result, nil
}

func (s *serverClient) ReadSnapshot(
	ctx context.Context,
	input eh.ReadSnapshotInput,
) (*eh.ReadSnapshotOutput, error) {
	s.logl.Debug.Printf("ReadSnapshot %s (%s)", input.Stream.String(), input.Perspective.String())

	output := &eh.ReadSnapshotOutput{}

	if _, err := ezhttp.Get(
		ctx,
		s.baseUrl+"/snapshot?stream="+url.QueryEscape(input.Stream.String())+"&perspective="+url.QueryEscape(input.Perspective.String()),
		ezhttp.AuthBearer(s.authToken),
		ezhttp.RespondsJson(output, false),
	); err != nil {
		if ezhttp.ErrorIs(err, http.StatusNotFound) {
			return nil, os.ErrNotExist
		} else {
			return nil, fmt.Errorf("ReadSnapshot(%s, %s): %w", input.Stream.String(), input.Perspective.String(), err)
		}
	}

	return output, nil
}

func (s *serverClient) WriteSnapshot(
	ctx context.Context,
	snapshot eh.PersistedSnapshot,
) error {
	s.logl.Debug.Printf("WriteSnapshot")

	if _, err := ezhttp.Put(
		ctx,
		s.baseUrl+"/snapshot",
		ezhttp.AuthBearer(s.authToken),
		ezhttp.SendJson(snapshot),
	); err != nil {
		return fmt.Errorf(
			"WriteSnapshot(%s, %s): %w",
			snapshot.Cursor.Stream().String(),
			snapshot.Perspective,
			err)
	}

	return nil
}

func (s *serverClient) DeleteSnapshot(
	ctx context.Context,
	stream eh.StreamName,
	perspective eh.SnapshotPerspective,
) error {
	s.logl.Debug.Printf("DeleteSnapshot")

	if _, err := ezhttp.Del(
		ctx,
		s.baseUrl+"/snapshot?stream="+url.QueryEscape(stream.String())+"&perspective="+url.QueryEscape(perspective.String()),
		ezhttp.AuthBearer(s.authToken),
	); err != nil {
		if ezhttp.ErrorIs(err, http.StatusNotFound) {
			return os.ErrNotExist
		} else {
			return fmt.Errorf("DeleteSnapshot: %w", err)
		}
	}

	return nil
}

// export EVENTHORIZON="http://:<apikey>@localhost"
func SplitAuthTokenFromUrl(rawUrl string) (string, string, error) {
	urlParsed, err := url.Parse(rawUrl)
	if err != nil {
		return "", "", err
	}

	if urlParsed.User == nil {
		return "", "", errors.New("auth token not set")
	}

	if urlParsed.User.Username() != "" {
		return "", "", fmt.Errorf("specifying username is not supported: %s", rawUrl)
	}

	authToken, _ := urlParsed.User.Password()
	if authToken == "" {
		return "", "", errors.New("auth token not set")
	}

	// remove user info from URL
	urlParsed.User = nil

	return urlParsed.String(), authToken, nil
}
