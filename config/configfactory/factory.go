package configfactory

// Resolves & caches cluster-wide configuration from scalablestore

import (
	"encoding/json"
	"github.com/function61/pyramid/config"
	ctypes "github.com/function61/pyramid/config/types"
	"github.com/function61/pyramid/scalablestore"
	"io"
	"io/ioutil"
	"log"
	"os"
)

const (
	DiscoveryFileRemotePath = "/_discovery.json"

	discoveryfileCachePath = "/tmp/discovery.json"
)

func Build() *config.Context {
	discovery, err := readCachedDiscovery()
	if err != nil {
		if !os.IsNotExist(err) {
			panic(err)
		}

		// was NotExist => read discovery information from scalablestore

		if err := storeCachedDiscovery(); err != nil {
			panic(err)
		}

		discovery, err = readCachedDiscovery()
		if err != nil {
			// was really supposed to succeed now
			panic(err)
		}
	}

	return config.NewContext(discovery)
}

func readCachedDiscovery() (*ctypes.DiscoveryFile, error) {
	buf, err := ioutil.ReadFile(discoveryfileCachePath)
	if err != nil {
		return nil, err
	}

	df := ctypes.DiscoveryFile{}

	if err := json.Unmarshal(buf, &df); err != nil {
		return nil, err
	}

	return &df, nil
}

func storeCachedDiscovery() error {
	log.Printf("configfactory: downloading discovery file")

	s3 := scalablestore.NewS3Manager()

	response, err := s3.Get(DiscoveryFileRemotePath)
	if err != nil {
		return err
	}

	fd, err := os.Create(discoveryfileCachePath)
	if err != nil {
		return err
	}
	defer fd.Close()

	if _, err := io.Copy(fd, response.Body); err != nil {
		return err
	}

	return nil
}
