package writerproxyclient

// To issue any commands to Event Horizon, you must contact a Writer. But that means
// that you have to have correct TLS configuration, know the authentication tokens,
// know the IP addresses of all the Writers, which Writers are responsible for
// which streams etc.
//
// To make things easier, the Pusher mirrors the API surface of a Writer, and
// handles all that above stuff for you, so you can just issue HTTP requests to
// a hardcoded IP (loopback) which will proxy your commands to a Writer.
//
// This is the client portion for this described setup.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	wtypes "github.com/function61/eventhorizon/pkg/legacy/writer/types"
)

type Client struct {
}

func New() *Client {
	return &Client{}
}

func (c *Client) CreateStream(req *wtypes.CreateStreamRequest) (*wtypes.CreateStreamOutput, error) {
	reqJson, _ := json.Marshal(req)

	resJson, _, err := c.handleAndReturnBodyAndStatusCode(c.url("/writer/create_stream"), reqJson, http.StatusCreated)
	if err != nil {
		return nil, err
	}

	var output wtypes.CreateStreamOutput
	if err := json.Unmarshal(resJson, &output); err != nil {
		return nil, err
	}

	return &output, nil
}

func (c *Client) Append(req *wtypes.AppendToStreamRequest) (*wtypes.AppendToStreamOutput, error) {
	reqJson, _ := json.Marshal(req)

	resJson, _, err := c.handleAndReturnBodyAndStatusCode(c.url("/writer/append"), reqJson, http.StatusCreated)
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

	return c.handleSuccessOnly(c.url("/writer/subscribe"), reqJson, http.StatusOK)
}

func (c *Client) UnsubscribeFromStream(req *wtypes.UnsubscribeFromStreamRequest) error {
	reqJson, _ := json.Marshal(req)

	return c.handleSuccessOnly(c.url("/writer/unsubscribe"), reqJson, http.StatusOK)
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

	client := &http.Client{}

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
		return body, resp.StatusCode, fmt.Errorf("HTTP %s: %s", resp.Status, body)
	}

	return body, resp.StatusCode, nil
}

func (c *Client) url(path string) string {
	return "http://127.0.0.1:9093" + path
}
