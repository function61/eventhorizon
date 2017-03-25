package main

import (
	"bytes"
	"encoding/json"
	"github.com/function61/pyramid/cli"
	"github.com/function61/pyramid/config/configfactory"
	ctypes "github.com/function61/pyramid/config/types"
	"github.com/function61/pyramid/scalablestore"
	"github.com/function61/pyramid/util/cryptorandombytes"
	"github.com/function61/pyramid/util/sslca"
	"log"
)

func writerBootstrap(args []string) error {
	if len(args) != 1 {
		return usage("<WriterIp>")
	}

	if err := cli.CheckForS3AccessKeys(); err != nil {
		log.Fatalf("main: %s", err.Error())
	}

	writerIp := args[0]

	log.Printf("bootstrap: generating certificate authority")

	caCert, caPrivateKey := sslca.GenerateCaCert()

	log.Printf("bootstrap: generating auth token")

	authToken := cryptorandombytes.Hex(16)

	log.Printf("bootstrap: generating discovery file")

	discoveryFile := ctypes.DiscoveryFile{
		WriterIp:      writerIp,
		AuthToken:     authToken,
		CaCertificate: string(caCert),
		CaPrivateKey:  string(caPrivateKey),
	}

	discoveryFileJson, err := json.MarshalIndent(discoveryFile, "", "    ")
	if err != nil {
		panic(err)
	}

	log.Printf("bootstrap: uploading discovery file to scalablestore")

	confCtx := configfactory.NewBootstrap()

	s3 := scalablestore.NewS3Manager(confCtx)

	if err := s3.Put(configfactory.DiscoveryFileRemotePath, bytes.NewReader(discoveryFileJson)); err != nil {
		panic(err)
	}

	log.Printf("bootstrap: bootstrapped Writer cluster for ip %s", writerIp)

	return nil
}
