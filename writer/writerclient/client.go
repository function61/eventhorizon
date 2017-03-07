package writerclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/function61/eventhorizon/config"
	writertypes "github.com/function61/eventhorizon/writer/writerhttp/types"
	"io/ioutil"
	"log"
	"net/http"
)

type Client struct {
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) Append(asr *writertypes.AppendToStreamRequest) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/append", config.WRITER_HTTP_PORT)

	asJson, _ := json.Marshal(asr)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(asJson))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil { // this is only network level errors
		panic(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 { // TODO: check for 2xx
		panic(errors.New("HTTP response not 201"))
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	log.Printf("WriterClient: %s response %s", url, body)

	return nil
}
