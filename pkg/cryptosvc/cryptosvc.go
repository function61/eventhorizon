package cryptosvc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"io"
	"log"
	"sync"

	"github.com/function61/eventhorizon/pkg/envelopeenc"
	"github.com/function61/eventhorizon/pkg/policy"
	"github.com/function61/gokit/cryptoutil"
	"github.com/function61/gokit/logex"
)

// imagine this being a HTTP client
type Service struct {
	unsealedPrivateKeys map[string]*rsa.PrivateKey // keyed by public key fingerprint
	initMu              sync.Mutex
	logl                *logex.Leveled
}

func New(logger *log.Logger) *Service {
	return &Service{
		unsealedPrivateKeys: map[string]*rsa.PrivateKey{},
		logl:                logex.Levels(logger),
	}
}

func (s *Service) initIfRequired() error {
	s.initMu.Lock()
	defer s.initMu.Unlock()

	if len(s.unsealedPrivateKeys) == 0 {
		return s.loadKeysAsIfFromEnv()
	}

	return nil
}

func (s *Service) NewAes256DekInEnvelope(
	ctx context.Context,
	resource policy.ResourceName,
) (*envelopeenc.Envelope, error) {
	if err := s.initIfRequired(); err != nil {
		return nil, err
	}

	pubKeys := []*rsa.PublicKey{}
	for _, priv := range s.unsealedPrivateKeys {
		pubKeys = append(pubKeys, &priv.PublicKey)
	}

	dek := make([]byte, 256/8)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, err
	}

	dekEnvelope, err := envelopeenc.Encrypt(dek, pubKeys, resource.String())
	if err != nil {
		return nil, err
	}

	return dekEnvelope, nil
}

func (s *Service) DecryptEnvelope(ctx context.Context, envelope envelopeenc.Envelope) ([]byte, error) {
	if err := s.initIfRequired(); err != nil {
		return nil, err
	}

	s.logl.Debug.Printf("decrypting envelope %s", envelope.Label)

	return envelope.Decrypt(func(kekId string) *rsa.PrivateKey {
		return s.unsealedPrivateKeys[kekId]
	})
}

func (s *Service) loadKeysAsIfFromEnv() error {
	return s.insertUnsealedPrivateKey(`-----BEGIN RSA PRIVATE KEY-----
MIICXAIBAAKBgQCqGKukO1De7zhZj6+H0qtjTkVxwTCpvKe4eCZ0FPqri0cb2JZfXJ/DgYSF6vUp
wmJG8wVQZKjeGcjDOL5UlsuusFncCzWBQ7RKNUSesmQRMSGkVb1/3j+skZ6UtW+5u09lHNsj6tQ5
1s1SPrCBkedbNf0Tp0GbMJDyR4e9T04ZZwIDAQABAoGAFijko56+qGyN8M0RVyaRAXz++xTqHBLh
3tx4VgMtrQ+WEgCjhoTwo23KMBAuJGSYnRmoBZM3lMfTKevIkAidPExvYCdm5dYq3XToLkkLv5L2
pIIVOFMDG+KESnAFV7l2c+cnzRMW0+b6f8mR1CJzZuxVLL6Q02fvLi55/mbSYxECQQDeAw6fiIQX
GukBI4eMZZt4nscy2o12KyYner3VpoeE+Np2q+Z3pvAMd/aNzQ/W9WaI+NRfcxUJrmfPwIGm63il
AkEAxCL5HQb2bQr4ByorcMWm/hEP2MZzROV73yF41hPsRC9m66KrheO9HPTJuo3/9s5p+sqGxOlF
L0NDt4SkosjgGwJAFklyR1uZ/wPJjj611cdBcztlPdqoxssQGnh85BzCj/u3WqBpE2vjvyyvyI5k
X6zk7S0ljKtt2jny2+00VsBerQJBAJGC1Mg5Oydo5NwD6BiROrPxGo2bpTbu/fhrT8ebHkTz2epl
U9VQQSQzY1oZMVX8i1m5WUTLPz2yLJIBQVdXqhMCQBGoiuSoSjafUhV7i1cEGpb88h5NBYZzWXGZ
37sJ5QsW+sJyoNde3xH8vdXhzU7eT82D6X/scw9RZz+/6rCJ4p0=
-----END RSA PRIVATE KEY-----`)
}

func (s *Service) insertUnsealedPrivateKey(pem string) error {
	pk, err := cryptoutil.ParsePemPkcs1EncodedRsaPrivateKey([]byte(pem))
	if err != nil {
		return err
	}

	id, err := cryptoutil.Sha256FingerprintForPublicKey(&pk.PublicKey)
	if err != nil {
		return err
	}

	s.unsealedPrivateKeys[id] = pk

	return nil
}
