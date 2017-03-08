package writerclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/function61/eventhorizon/config"
	"github.com/function61/eventhorizon/cursor"
	rtypes "github.com/function61/eventhorizon/reader/types"
	wtypes "github.com/function61/eventhorizon/writer/writerhttp/types"
	"io/ioutil"
	"log"
	"net/http"
)

type Client struct {
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) LiveRead(input *wtypes.LiveReadInput) (*rtypes.ReadResult, error) {
	cur := cursor.CursorFromserializedMust(input.Cursor)
	url := fmt.Sprintf("http://%s:%d/liveread", cur.Server, config.WRITER_HTTP_PORT)

	reqJson, _ := json.Marshal(input)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqJson))
	// req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil { // this is only network level errors
		panic(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 { // TODO: check for 2xx
		panic(errors.New("HTTP response not 200"))
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	readResponseFromJson := &types.ReadResult{}

	if err := json.Unmarshal(body, readResponseFromJson); err != nil {
		panic(err)
	}

	return readResponseFromJson, nil
}

func (c *Client) Append(asr *wtypes.AppendToStreamRequest) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/append", config.WRITER_HTTP_PORT)

	asJson, _ := json.Marshal(asr)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(asJson))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil { // this is only network level errors
		panic(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// error reading response body (should not happen even with HTTP errors)
		// probably a network level error
		panic(err)
	}

	if resp.StatusCode != 201 { // TODO: check for 2xx
		panic(errors.New(fmt.Sprintf("HTTP %s: %s", resp.Status, body)))
	}

	log.Printf("WriterClient: %s response %s", url, body)

	return nil
}
