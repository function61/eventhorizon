package writerclient

import (
	"bytes"
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
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) LiveRead(input *wtypes.LiveReadInput) (reader io.Reader, wasFileNotExist bool, err error) {
	cur := cursor.CursorFromserializedMust(input.Cursor)
	url := fmt.Sprintf("http://%s:%d/liveread", cur.Server, config.WRITER_HTTP_PORT)

	reqJson, _ := json.Marshal(input)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(reqJson))
	applyAuthorizationHeader(req)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil { // this is only network level errors
		panic(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, false, err
	}

	if resp.StatusCode != 200 { // TODO: check for 2xx
		err := errors.New(fmt.Sprintf("HTTP %s: %s", resp.Status, body))

		if resp.StatusCode == http.StatusNotFound {
			return nil, true, err
		}

		// unexpected HTTP error
		return nil, false, err
	}

	return bytes.NewReader(body), false, nil
}

func (c *Client) CreateStream(asr *wtypes.CreateStreamRequest) error {
	reqJson, _ := json.Marshal(asr)

	return c.internalMakeRequest(c.url("/create_stream"), reqJson, http.StatusCreated)
}

func (c *Client) Append(asr *wtypes.AppendToStreamRequest) error {
	reqJson, _ := json.Marshal(asr)

	return c.internalMakeRequest(c.url("/append"), reqJson, http.StatusCreated)
}

func (c *Client) SubscribeToStream(req *wtypes.SubscribeToStreamRequest) error {
	reqJson, _ := json.Marshal(req)

	return c.internalMakeRequest(c.url("/subscribe"), reqJson, http.StatusOK)
}

func (c *Client) UnsubscribeFromStream(req *wtypes.UnsubscribeFromStreamRequest) error {
	reqJson, _ := json.Marshal(req)

	return c.internalMakeRequest(c.url("/unsubscribe"), reqJson, http.StatusOK)
}

func (c *Client) internalMakeRequest(url string, asJson []byte, expectedCode int) error {
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(asJson))
	applyAuthorizationHeader(req)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil { // this is only network level errors
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// error reading response body (should not happen even with HTTP errors)
		// probably a network level error
		return err
	}

	if resp.StatusCode != expectedCode { // TODO: check for 2xx
		return errors.New(fmt.Sprintf("HTTP %s: %s", resp.Status, body))
	}

	return nil
}

func (c *Client) url(path string) string {
	url := fmt.Sprintf("http://127.0.0.1:%d%s", config.WRITER_HTTP_PORT, path)

	return url
}

func applyAuthorizationHeader(req *http.Request) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", config.AUTH_TOKEN))
}
