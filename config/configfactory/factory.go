package configfactory

// Resolves & caches cluster-wide configuration from scalablestore

import (
	"encoding/json"
	"github.com/function61/eventhorizon/config"
	ctypes "github.com/function61/eventhorizon/config/types"
	"github.com/function61/eventhorizon/scalablestore"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
)

const (
	DiscoveryFileRemotePath = "/_discovery.json"
)

func BuildMust() *config.Context {
	ctx, err := Build()
	if err != nil {
		panic(err)
	}

	return ctx
}

func Build() (*config.Context, error) {
	if _, err := os.Stat(config.BoltDbDir); os.IsNotExist(err) {
		log.Printf("configfactory: mkdir %s", config.BoltDbDir)

		if err = os.MkdirAll(config.BoltDbDir, 0755); err != nil {
			panic(err)
		}
	}

	discovery, err := readCachedDiscovery()
	if err != nil {
		if !os.IsNotExist(err) {
			panic(err)
		}

		// was NotExist => read discovery information from scalablestore

		if err := retrieveDiscoveryAndCache(NewBootstrap()); err != nil {
			// not found => bootstrap file not present
			return nil, err
		}

		discovery, err = readCachedDiscovery()
		if err != nil {
			// was really supposed to succeed now
			panic(err)
		}
	}

	return config.NewContext(discovery, parseS3Url()), nil
}

// needed for accessing S3 pre-discovery-file-download
func NewBootstrap() *config.Context {
	return config.NewContext(nil, parseS3Url())
}

func readCachedDiscovery() (*ctypes.DiscoveryFile, error) {
	buf, err := ioutil.ReadFile(config.BoltDbDir + "/_discovery.json")
	if err != nil {
		return nil, err
	}

	df := ctypes.DiscoveryFile{}

	if err := json.Unmarshal(buf, &df); err != nil {
		return nil, err
	}

	return &df, nil
}

func retrieveDiscoveryAndCache(confCtx *config.Context) error {
	log.Printf("configfactory: downloading discovery file")

	s3 := scalablestore.NewS3Manager(confCtx)

	response, err := s3.Get(DiscoveryFileRemotePath)
	if err != nil {
		return err
	}

	fd, err := os.Create(config.BoltDbDir + "/_discovery.json")
	if err != nil {
		return err
	}
	defer fd.Close()

	if _, err := io.Copy(fd, response.Body); err != nil {
		return err
	}

	return nil
}

// s3://keyid:keysecret@us-east-1/eventhorizon.fn61.net
func parseS3Url() *url.URL {
	storeSpec := os.Getenv("STORE")
	if storeSpec == "" {
		panic("STORE undefined")
	}

	urlParsed, err := url.Parse(storeSpec)
	if err != nil {
		panic(err)
	}

	if urlParsed.Scheme != "s3" {
		panic("only supporting S3 for now")
	}

	if urlParsed.User == nil {
		panic("Failed to parse Userinfo portion of the S3 URL")
	}

	return urlParsed
}
