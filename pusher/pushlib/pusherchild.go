package pushlib

// A convenience feature for automatically starting and stopping the Pusher
// component alongside with your app start/stop. Just call StartChildProcess()
// from your app and the lifecycle of Pusher is automatically managed for you.
//
// You could also easily just start pusher manually/automatically by other
// mechanisms, so you don't need this file.
//
// Note: a security drawback of using this is that because we start Pusher from
// your application, your application has to know the "STORE" variable. If you
// manage Pusher separately, then your application won't see the "master key".

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

	// keep dummy STDIN pipe open, so child (Pusher) can detect
	// parent (us) dying by EOF and stop
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
