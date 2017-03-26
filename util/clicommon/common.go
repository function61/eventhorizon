package clicommon

import (
	"errors"
	"os"
	"os/signal"
	"syscall"
)

func CheckForS3AccessKeys() error {
	if val := os.Getenv("STORE"); val == "" {
		return errors.New("Need STORE for S3 access. See README.md")
	}

	return nil
}

func WaitForInterrupt() os.Signal {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	return <-ch
}

func WaitForStdinEof() {
	buf := make([]byte, 256)

	for {
		if _, err := os.Stdin.Read(buf); err != nil {
			// assume io.EOF. if we just compared to EOF,
			// we would loop forever on other errors
			break
		}
	}
}
