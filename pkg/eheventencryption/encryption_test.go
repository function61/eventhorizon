package eheventencryption

import (
	"bytes"
	"io"
	"testing"

	"github.com/function61/gokit/testing/assert"
)

func TestEncryptDecrypt(t *testing.T) {
	dek := dummyDek()

	encryptedEvents, err := encryptWithRand([]string{"foo", "bar"}, dek, nullIv())
	assert.Ok(t, err)

	// IV is stored as prefix, which is now easy to spot as "AAA.."
	assert.EqualJson(t, encryptedEvents, `{
  "Kind": 2,
  "Raw": "AAAAAAAAAAAAAAAAAAAAAAAAIj2JpVt10A=="
}`)

	events, err := Decrypt(*encryptedEvents, dek)
	assert.Ok(t, err)

	assert.EqualJson(t, events, `[
  "foo",
  "bar"
]`)
}

func TestCompression(t *testing.T) {
	dek := dummyDek()

	encryptedEvents, err := encryptWithRand([]string{"fooooooooooooooooooooooooooooooooooooooooooo"}, dek, nullIv())
	assert.Ok(t, err)

	// remember the long "AAAA".. from earlier test? the difference below is for the
	// CompressionMethod=deflate being specified in the header
	assert.EqualJson(t, encryptedEvents, `{
  "Kind": 2,
  "Raw": "AQAAAAAAAAAAAAAAAAAAAAAADpnBsTkWorvMSQ=="
}`)

	events, err := Decrypt(*encryptedEvents, dek)
	assert.Ok(t, err)

	assert.EqualJson(t, events, `[
  "fooooooooooooooooooooooooooooooooooooooooooo"
]`)
}

// dangerous in production, but we'll use this in our test so the base64 is distinctive
func nullIv() io.Reader {
	return bytes.NewReader([]byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	})
}

func dummyDek() []byte {
	return []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
		0x17, 0x18, 0x19, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x30, 0x31, 0x32,
	}
}
