package cli

import (
	"errors"
	"os"
	"os/signal"
	"syscall"
)

func CheckForS3AccessKeys() error {
	if val := os.Getenv("AWS_ACCESS_KEY_ID"); val == "" {
		return errors.New("Need AWS_ACCESS_KEY_ID for S3 access. See README.md")
	}

	if val := os.Getenv("AWS_SECRET_ACCESS_KEY"); val == "" {
		return errors.New("Need AWS_SECRET_ACCESS_KEY for S3 access. See README.md")
	}

	return nil
}

func WaitForInterrupt() os.Signal {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	return <-ch
}
