package keyserver

import (
	"crypto/rand"
	"io"

	"github.com/function61/eventhorizon/pkg/policy"
	"github.com/function61/gokit/crypto/envelopeenc"
)

func MakeDekEnvelope(
	dek []byte,
	resource policy.ResourceName,
	slotEncrypters []envelopeenc.SlotEncrypter,
) (*envelopeenc.Envelope, error) {
	dekEnvelope, err := envelopeenc.Encrypt(dek, slotEncrypters, resource.String())
	if err != nil {
		return nil, err
	}

	return dekEnvelope, nil
}

func NewDek() ([]byte, error) {
	dek := make([]byte, 256/8)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, err
	}

	return dek, nil
}
