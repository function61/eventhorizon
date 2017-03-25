package writerclient

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/cursor"
	wtypes "github.com/function61/pyramid/writer/types"
	"io"
	"io/ioutil"
	"net/http"
)

type Client struct {
	confCtx      *config.Context
	tlsTransport *http.Transport
}

func New(confCtx *config.Context) *Client {
	tlsTransport := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: confCtx.GetCaCertificates(),
		},
	}

	return &Client{
		confCtx:      confCtx,
		tlsTransport: tlsTransport,
	}
}

func (c *Client) LiveRead(input *wtypes.LiveReadInput) (reader io.Reader, wasFileNotExist bool, err error) {
	cur := cursor.CursorFromserializedMust(input.Cursor)
	reqJson, _ := json.Marshal(input)

	body, statusCode, err := c.handleAndReturnBodyAndStatusCode(c.url(cur.Server, "/liveread"), reqJson, http.StatusOK)

	if err != nil {
		wasFileNotExist := statusCode == http.StatusNotFound
		return nil, wasFileNotExist, err
	}

	return bytes.NewReader(body), false, nil
}

func (c *Client) CreateStream(req *wtypes.CreateStreamRequest) error {
	reqJson, _ := json.Marshal(req)

	return c.handleSuccessOnly(c.url("", "/create_stream"), reqJson, http.StatusCreated)
}

func (c *Client) Append(req *wtypes.AppendToStreamRequest) (*wtypes.AppendToStreamOutput, error) {
	reqJson, _ := json.Marshal(req)

	resJson, _, err := c.handleAndReturnBodyAndStatusCode(c.url("", "/append"), reqJson, http.StatusCreated)
	if err != nil {
		return nil, err
	}

	var output wtypes.AppendToStreamOutput
	if err := json.Unmarshal(resJson, &output); err != nil {
		return nil, err
	}

	return &output, nil
}

func (c *Client) SubscribeToStream(req *wtypes.SubscribeToStreamRequest) error {
	reqJson, _ := json.Marshal(req)

	return c.handleSuccessOnly(c.url("", "/subscribe"), reqJson, http.StatusOK)
}

func (c *Client) UnsubscribeFromStream(req *wtypes.UnsubscribeFromStreamRequest) error {
	reqJson, _ := json.Marshal(req)

	return c.handleSuccessOnly(c.url("", "/unsubscribe"), reqJson, http.StatusOK)
}

// less specific version for callers that are only interested about success, but
// not particular failure reasons or response body
func (c *Client) handleSuccessOnly(url string, asJson []byte, expectedCode int) error {
	_, _, err := c.handleAndReturnBodyAndStatusCode(url, asJson, expectedCode)
	return err
}

// more specific version for callers that are interested of failure reason and status
func (c *Client) handleAndReturnBodyAndStatusCode(url string, requestBody []byte, expectedCode int) (body []byte, statusCode int, err error) {
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.confCtx.AuthToken()))

	client := &http.Client{
		Transport: c.tlsTransport,
	}

	resp, networkErr := client.Do(req)
	if networkErr != nil { // this is only network level errors
		return nil, 0, networkErr
	}
	defer resp.Body.Close()

	body, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		// error reading response body (should not happen even with HTTP errors)
		// probably a network level error
		return nil, 0, readErr
	}

	if resp.StatusCode != expectedCode {
		return body, resp.StatusCode, errors.New(fmt.Sprintf("HTTP %s: %s", resp.Status, body))
	}

	return body, resp.StatusCode, nil
}

func (c *Client) url(server string, path string) string {
	if server == "" {
		server = c.confCtx.GetWriterIp()
	}

	// looks like "127.0.0.1:9092"
	writerServerAddr := fmt.Sprintf("%s:%d", server, c.confCtx.GetWriterPort())

	return "https://" + writerServerAddr + path
}
