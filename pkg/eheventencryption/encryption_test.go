package eheventencryption

import (
	"bytes"
	"io"
	"testing"

	"github.com/function61/gokit/assert"
)

func TestEncryptDecrypt(t *testing.T) {
	dek := dummyDek()

	encryptedEvents, err := encryptWithRand([]string{"foo", "bar"}, dek, nullIv())
	assert.Ok(t, err)

	// IV is stored as prefix, which is now easy to spot as "AAA.."
	assert.EqualJson(t, encryptedEvents, `{
  "Kind": 2,
  "Raw": "AAAAAAAAAAAAAAAAAAAAACI9iaVbddA="
}`)

	events, err := Decrypt(*encryptedEvents, dek)
	assert.Ok(t, err)

	assert.EqualJson(t, events, `[
  "foo",
  "bar"
]`)
}

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
