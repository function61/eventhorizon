package wal

import (
	"log"
	"os"
	"strings"
)

type WalGuardedFile struct {
	nextFreePosition uint64
	walSize          uint64
	fd               *os.File

	// TODO: needed only for debugging, and not actual filename
	fileNameFictional string
}

func WalGuardedFileOpen(directory string, fileName string) *WalGuardedFile {
	// "/tenants/foo/_/0.log" => "_tenants_foo___0.log"
	fileNameSafe := strings.Replace(fileName, "/", "_", -1)

	chunkPath := directory + "/" + fileNameSafe

	fd, err := os.OpenFile(chunkPath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}

	log.Printf("WalGuardedFile: Opened %s", chunkPath)

	stats, err := fd.Stat()
	if err != nil {
		panic(err)
	}

	return &WalGuardedFile{
		nextFreePosition:  uint64(stats.Size()),
		fd:                fd,
		walSize:           0, // filled on WAL scanning
		fileNameFictional: fileName,
	}
}

func (w *WalGuardedFile) Close() error {
	log.Printf("WalGuardedFile: closing %s", w.fileNameFictional)

	return w.fd.Close()
}
