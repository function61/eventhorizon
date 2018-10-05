package sslca

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"time"
)

// Package for managing a CA and signing server certs.
// Used in a setting where we control both the servers and clients.
// Some code borrowed from https://golang.org/src/crypto/tls/generate_cert.go

const (
	organisationName = "Event Horizon internal CA"
)

// Generates a CA suitable for signing other certs
func GenerateCaCert() ([]byte, []byte) {
	// why EC: https://blog.cloudflare.com/ecdsa-the-digital-signature-algorithm-of-a-better-internet/
	caPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}

	notBefore := time.Now().Add(time.Hour * -1) // account for clock drift
	notAfter := notBefore.AddDate(20, 0, 0)     // years

	certTemplate := x509.Certificate{
		SerialNumber: generateSerialNumber(),
		Subject: pkix.Name{
			Organization: []string{organisationName},
		},

		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},

		// means: "look at IsCA field also"
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &certTemplate, &certTemplate, publicKey(caPrivateKey), caPrivateKey)
	if err != nil {
		log.Fatalf("Failed to create certificate: %s", err)
	}

	certBuffer := bytes.Buffer{}
	pem.Encode(&certBuffer, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	keyBuffer := bytes.Buffer{}
	pem.Encode(&keyBuffer, pemBlockForKey(caPrivateKey))

	return certBuffer.Bytes(), keyBuffer.Bytes()
}

// signs a server certificate for an IP address
func SignServerCert(ipSerialized string, caSerialized string, caPrivateKeySerialized string) ([]byte, []byte) {
	ca, err := parsePemCertificate(caSerialized)
	if err != nil {
		panic(err)
	}
	caPrivateKey := parsePrivateKey(caPrivateKeySerialized)

	serverPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}

	notBefore := time.Now().Add(time.Hour * -1) // account for clock drift
	notAfter := notBefore.AddDate(20, 0, 0)     // years

	ip := net.ParseIP(ipSerialized)
	if ip == nil {
		panic("failed to parse ip")
	}

	certTemplate := &x509.Certificate{
		SerialNumber: generateSerialNumber(),
		Subject: pkix.Name{
			Organization: []string{organisationName},
		},

		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},

		IPAddresses: []net.IP{ip},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, certTemplate, ca, publicKey(serverPrivateKey), caPrivateKey)
	if err != nil {
		panic(err)
	}

	certBuffer := bytes.Buffer{}
	pem.Encode(&certBuffer, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	keyBuffer := bytes.Buffer{}
	pem.Encode(&keyBuffer, pemBlockForKey(serverPrivateKey))

	return certBuffer.Bytes(), keyBuffer.Bytes()
}

func publicKey(priv interface{}) interface{} {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &k.PublicKey
	case *ecdsa.PrivateKey:
		return &k.PublicKey
	default:
		return nil
	}
}

func pemBlockForKey(priv interface{}) *pem.Block {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}
	case *ecdsa.PrivateKey:
		b, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to marshal ECDSA private key: %v", err)
			os.Exit(2)
		}
		return &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}
	default:
		return nil
	}
}

func generateSerialNumber() *big.Int {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		log.Fatalf("failed to generate serial number: %s", err)
	}

	return serialNumber
}

func parsePemCertificate(certPem string) (*x509.Certificate, error) {
	block, _ := pem.Decode([]byte(certPem))
	if block == nil {
		return nil, errors.New("failed to parse PEM")
	}
	if block.Type != "CERTIFICATE" {
		panic("expecting a certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}

	return cert, nil
}

func parsePrivateKey(serialized string) *ecdsa.PrivateKey {
	block, _ := pem.Decode([]byte(serialized))
	if block == nil {
		panic("failed to parse privkey PEM")
	}

	// TODO: why cannot this be done generically?
	// probably need pemBlockForKey() for other way around
	privKey, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		panic(err)
	}

	return privKey
}
