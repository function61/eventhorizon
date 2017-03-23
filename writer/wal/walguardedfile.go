package wal

import (
	"github.com/function61/pyramid/config"
	"log"
	"os"
	"strings"
)

type WalGuardedFile struct {
	nextFreePosition uint64
	walSize          uint64
	fd               *os.File

	// This is original filename (not real filename)
	fileNameFictional string // "/tenants/foo/_/0.log"
}

func WalGuardedFileOpen(fileName string) *WalGuardedFile {
	fd, err := os.OpenFile(computeInternalPath(fileName), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}

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

// Returns the internal path by which the file actually resides in the filesystem.
//
// TODO: have Writer dictate a safe internal path for WAL usage and not do this
//       ourselves.
func (w *WalGuardedFile) GetInternalRealPath() string {
	return computeInternalPath(w.fileNameFictional)
}

func (w *WalGuardedFile) Close() error {
	log.Printf("WalGuardedFile: closing %s", w.fileNameFictional)

	return w.fd.Close()
}

// "/foo/bar.txt" => "/full/path-to-wal/_foo_bar.txt"
func computeInternalPath(fileName string) string {
	// "/tenants/foo/_/0.log" => "_tenants_foo___0.log"
	fileNameSafe := strings.Replace(fileName, "/", "_", -1)

	return config.WalManagerDataDir + "/" + fileNameSafe
}
