package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"github.com/function61/pyramid/cli"
	"github.com/function61/pyramid/config/configfactory"
	ctypes "github.com/function61/pyramid/config/types"
	"github.com/function61/pyramid/scalablestore"
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

	discoveryFile := ctypes.DiscoveryFile{
		WriterIp:  writerIp,
		AuthToken: generateAuthToken(16),
	}

	discoveryFileJson, err := json.Marshal(discoveryFile)
	if err != nil {
		panic(err)
	}

	confCtx := configfactory.NewBootstrap()

	s3 := scalablestore.NewS3Manager(confCtx)

	if err := s3.Put(configfactory.DiscoveryFileRemotePath, bytes.NewReader(discoveryFileJson)); err != nil {
		panic(err)
	}

	log.Printf("bootstrap: bootstrapped Writer cluster with %s", string(discoveryFileJson))

	return nil
}

func generateAuthToken(bytesLen int) string {
	randBytes := make([]byte, bytesLen)

	if _, err := rand.Read(randBytes); err != nil {
		panic(err)
	}

	return hex.EncodeToString(randBytes)
}
