package pushlib

import (
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
)

func StartChildProcess(url string) {
	log.Printf("pusherchild: starting")

	cmd := exec.Command("pyramid", "pusher", "y", url)

	// keep dummy STDIN pipe open, so child can detect parent dying by EOF
	_, err := cmd.StdinPipe()
	if err != nil {
		panic(err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		panic(err)
	}

	if err := cmd.Start(); err != nil {
		panic(err)
	}

	pipeStdoutAndStderrStreams(stdout, stderr).Wait()

	if err := cmd.Wait(); err != nil {
		log.Printf("pusherchild: exited with error: %s", err.Error())
	}

	log.Printf("pusherchild: exited")
}

func pipeStdoutAndStderrStreams(stdout io.ReadCloser, stderr io.ReadCloser) *sync.WaitGroup {
	stdStreamsReadingDone := &sync.WaitGroup{}

	stdStreamsReadingDone.Add(1)
	go func() {
		defer stdStreamsReadingDone.Done()

		if _, err := io.Copy(os.Stdout, stdout); err != nil {
			log.Printf("pusherchild: stdout copy error: %s", err.Error())
		}
	}()

	stdStreamsReadingDone.Add(1)
	go func() {
		defer stdStreamsReadingDone.Done()

		if _, err := io.Copy(os.Stderr, stderr); err != nil {
			log.Printf("pusherchild: stderr copy error: %s", err.Error())
		}
	}()

	return stdStreamsReadingDone
}
