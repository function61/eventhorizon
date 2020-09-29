package keyserver

import (
	"context"
	"io/ioutil"
	"log"

	"github.com/function61/gokit/crypto/envelopeenc"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/net/http/ezhttp"
)

func NewClient(serverUrl string, authToken string, logger *log.Logger) Unsealer {
	return &Client{
		serverUrl: serverUrl,
		authToken: authToken,
		logl:      logex.Levels(logger),
	}
}

// implements Unsealer by calling a remote keyserver
type Client struct {
	serverUrl string
	authToken string
	logl      *logex.Leveled
}

func (c *Client) UnsealEnvelope(ctx context.Context, envelope envelopeenc.Envelope) ([]byte, error) {
	c.logl.Debug.Printf("UnsealEnvelope %s", envelope.Label)

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
