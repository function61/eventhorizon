package keyserver

import (
	"context"
	"io/ioutil"

	"github.com/function61/gokit/crypto/envelopeenc"
	"github.com/function61/gokit/net/http/ezhttp"
)

func NewClient(serverUrl string, authToken string) Unsealer {
	return &Client{
		serverUrl: serverUrl,
		authToken: authToken,
	}
}

// implements Unsealer by calling a remote keyserver
type Client struct {
	serverUrl string
	authToken string
}

func (c *Client) UnsealEnvelope(ctx context.Context, envelope envelopeenc.Envelope) ([]byte, error) {
	res, err := ezhttp.Post(
		ctx,
		c.serverUrl+"/keyserver/envelope-decrypt",
		ezhttp.AuthBearer(c.authToken),
		ezhttp.SendJson(envelope))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	unencrypted, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return unencrypted, nil
}
