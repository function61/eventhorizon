package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	ptypes "github.com/function61/eventhorizon/pkg/legacy/pusher/types"
)

// transports Pushes via HTTP+JSON

type HttpJsonTransport struct {
	address string
}

func NewHttpJsonTransport(address string) *HttpJsonTransport {
	return &HttpJsonTransport{
		address: address,
	}
}

func (h *HttpJsonTransport) Push(input *ptypes.PushInput) (*ptypes.PushOutput, error) {
	requestBody, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	req, _ := http.NewRequest("POST", h.address, bytes.NewBuffer(requestBody))

	// req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", "foo"))

	client := &http.Client{}
	resp, networkErr := client.Do(req)
	if networkErr != nil { // this is only network level errors
		return nil, networkErr
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errorBody, _ := ioutil.ReadAll(resp.Body)

		return nil, fmt.Errorf(
			"HTTP %s: %s",
			resp.Status,
			string(errorBody))
	}

	var decodedResponse ptypes.PushOutput
	if err := json.NewDecoder(resp.Body).Decode(&decodedResponse); err != nil {
		return nil, err
	}

	return &decodedResponse, nil
}
