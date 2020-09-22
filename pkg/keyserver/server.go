// Encapsulates private key operations as its own service, so it can be ran on a server
// with reduced attack surface & close-to-bulletproof auditing
package keyserver

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/function61/gokit/crypto/cryptoutil"
	"github.com/function61/gokit/crypto/envelopeenc"
	"github.com/function61/gokit/log/logex"
)

type Unsealer interface {
	UnsealEnvelope(ctx context.Context, envelope envelopeenc.Envelope) ([]byte, error)
}

// TODO: rename
type Server struct {
	key  envelopeenc.SlotDecrypter
	logl *logex.Leveled
}

func NewServer(privateKeyPath string, logger *log.Logger) (Unsealer, error) {
	privKeyPem, err := ioutil.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("keyserver: %w", err)
	}

	privKey, err := cryptoutil.ParsePemPkcs1EncodedRsaPrivateKey(privKeyPem)
	if err != nil {
		return nil, fmt.Errorf("keyserver: %w", err)
	}

	return &Server{
		key:  envelopeenc.RsaOaepSha256Decrypter(privKey),
		logl: logex.Levels(logger),
	}, nil
}

func (l *Server) UnsealEnvelope(ctx context.Context, envelope envelopeenc.Envelope) ([]byte, error) {
	l.logl.Info.Printf("unsealing %s", envelope.Label)

	return envelope.Decrypt(l.key)
}
