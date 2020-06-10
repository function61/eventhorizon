package bootstrap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/function61/eventhorizon/pkg/legacy/config"
	"github.com/function61/eventhorizon/pkg/legacy/config/configfactory"
	ctypes "github.com/function61/eventhorizon/pkg/legacy/config/types"
	"github.com/function61/eventhorizon/pkg/legacy/scalablestore"
	"github.com/function61/eventhorizon/pkg/legacy/util/cryptorandombytes"
	"github.com/function61/eventhorizon/pkg/legacy/util/resolvepublicip"
	"github.com/function61/eventhorizon/pkg/legacy/util/sslca"
	"github.com/function61/eventhorizon/pkg/legacy/writer"
)

func Run() error {
	log.Printf("bootstrap: starting bootstrap process")

	bootstrapConf := configfactory.NewBootstrap()

	s3 := scalablestore.NewS3Manager(bootstrapConf)

	eligibleForBootstrap, err := s3.IsEligibleForBootstrap()
	if err != nil {
		return fmt.Errorf("bootstrap: eligibility check failed: %s", err.Error())
	}

	if !eligibleForBootstrap {
		return fmt.Errorf("bootstrap: NOT eligible for bootstrap")
	}

	if err := generateAndStoreDiscoveryFile(bootstrapConf, s3); err != nil {
		return err
	}

	stillEligibleForBootstrap, err := s3.IsEligibleForBootstrap()
	if err != nil {
		panic(err)
	}

	if stillEligibleForBootstrap {
		return fmt.Errorf("bootstrap: should not be eligible for bootstrap anymore")
	}

	// now we can build real configuration (which Writer needs)
	realConf := configfactory.BuildMust()

	wri := writer.New(realConf)

	log.Printf("bootstrap: creating / stream")

	if _, err := wri.CreateStream("/"); err != nil {
		return err
	}

	log.Printf("bootstrap: creating /_sub stream")

	if _, err := wri.CreateStream("/_sub"); err != nil {
		return err
	}

	wri.Close()

	return nil
}

func generateAndStoreDiscoveryFile(confCtx *config.Context, s3 *scalablestore.S3Manager) error {
	writerPublicIp, err := resolveWriterPublicIp()
	if err != nil {
		return err
	}

	log.Printf("bootstrap: public IP to advertise: %s", writerPublicIp)

	log.Printf("bootstrap: generating certificate authority")

	caCert, caPrivateKey := sslca.GenerateCaCert()

	log.Printf("bootstrap: generating auth token")

	authToken := cryptorandombytes.Hex(16)

	log.Printf("bootstrap: generating encryption master key")

	encryptionMasterKey := cryptorandombytes.Hex(config.AesKeyLenBytes)

	log.Printf("bootstrap: generating discovery file")

	discoveryFile := ctypes.DiscoveryFile{
		WriterIp:            writerPublicIp,
		AuthToken:           authToken,
		CaCertificate:       string(caCert),
		CaPrivateKey:        string(caPrivateKey),
		EncryptionMasterKey: encryptionMasterKey,
	}

	discoveryFileJson, err := json.MarshalIndent(discoveryFile, "", "    ")
	if err != nil {
		panic(err)
	}

	log.Printf("bootstrap: uploading discovery file to scalablestore")

	if err := s3.Put(configfactory.DiscoveryFileRemotePath, bytes.NewReader(discoveryFileJson)); err != nil {
		panic(err)
	}

	log.Printf("bootstrap: discovery file uploaded to scalablestore")

	return nil
}

func resolveWriterPublicIp() (string, error) {
	if ip := os.Getenv("WRITER_IP_TO_ADVERTISE"); ip != "" {
		return ip, nil
	}

	log.Printf("bootstrap: resolving public IP from %s", resolvepublicip.ServiceDisplayName)

	ip, err := resolvepublicip.Resolve()
	if err != nil {
		return "", fmt.Errorf("failed to resolve public IP: %s", err.Error())
	}

	return ip, nil
}
