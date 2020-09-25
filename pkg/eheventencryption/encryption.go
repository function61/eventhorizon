// Encrypts (& maybe compresses) line-based (\n) data (usually "ehevent" serialization format) into a LogData item
package eheventencryption

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/function61/eventhorizon/pkg/eh"
)

// format is:
//
// 1 byte     Header
//            |- upper 4 bits reserved. if non-zero, assume incompatible format version & stop decoding!
//            `- lowest 4 bits compression method (0x0 = uncompressed, 0x1 = deflate)
// 1-n bytes  Reserved for DEK version (key rotation). N is defined by encoding/binary.Uvarint semantics
// 16 bytes   IV
// rest       AES256_CTR(plaintextMaybeCompressed, dek).
//
// plaintext is multiple "ehevent" lines split by \n character.

type CompressionMethod byte

const (
	CompressionMethodNone    CompressionMethod = 0
	CompressionMethodDeflate CompressionMethod = 1
)

func Encrypt(lines []string, dek []byte) (*eh.LogData, error) {
	return encryptWithRand(lines, dek, rand.Reader)
}

func encryptWithRand(lines []string, dek []byte, cryptoRand io.Reader) (*eh.LogData, error) {
	if len(lines) == 0 {
		return nil, errors.New("EncryptedData: empty event slices are not supported")
	}

	plaintextMaybeCompressed, compressionMethod, err := compressIfWellCompressible(
		[]byte(strings.Join(lines, "\n")))
	if err != nil {
		return nil, err
	}

	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(cryptoRand, iv); err != nil {
		return nil, err
	}

	aesCipher, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}

	// header's upper 4 bits are currently zero (and compressionMethod uses lower 4 bits),
	// so our header is basically compressionMethod
	header := byte(compressionMethod)

	// the 0x00 for "reserved" semantics amount to high bit=varuint fits in 1 byte, and next
	// 7 bits being zero signals that we use DEK v0 (i.e. this is not a rotated DEK)
	raw := bytes.NewBuffer(append([]byte{header, 0x00}, iv...))

	if err := encryptStream(raw, bytes.NewReader(plaintextMaybeCompressed), cipher.NewCTR(aesCipher, iv)); err != nil {
		return nil, err
	}

	return &eh.LogData{
		Kind: eh.LogDataKindEncryptedData,
		Raw:  raw.Bytes(),
	}, nil
}

func Decrypt(data eh.LogData, dek []byte) ([]string, error) {
	if data.Kind != eh.LogDataKindEncryptedData {
		return nil, fmt.Errorf("Decrypt: kind not LogDataKindEncryptedData; got %d", data.Kind)
	}

	raw := bytes.NewReader(data.Raw)

	header, err := raw.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Decrypt: %w", err)
	}

	// upper 4 bits are reserved, and if any of them are set it indicates incompatible format
	// that we don't know how to decode yet
	if header&0xf0 != 0x00 {
		return nil, fmt.Errorf(
			"Decrypt: header reserved bits set - incompatible format: %x",
			header&0xf0)
	}

	compressionMethod := CompressionMethod(header & 0x0f)

	// DEK rotation not yet implemented - for now just asserting this to be 0. when implemented
	// we'd use encoding/binary.Uvarint, which is compatible with our stub approach because
	// var-length byte spans guarantee highest bit being 1 when bytesLen >= 2
	dekVersionReserved, err := raw.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Decrypt: %w", err)
	}

	if dekVersionReserved != 0x00 {
		return nil, errors.New("Decrypt: DEK rotation not implemented")
	}

	// after reading IV, next reads contain only ciphertext
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(raw, iv); err != nil {
		return nil, fmt.Errorf("Decrypt: read IV: %w", err)
	}

	aesCipher, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("Decrypt: %w", err)
	}

	plaintextMaybeCompressed := &cipher.StreamReader{
		S: cipher.NewCTR(aesCipher, iv),
		R: raw,
	}

	plaintext, err := func() (io.ReadCloser, error) {
		switch compressionMethod {
		case CompressionMethodNone:
			return ioutil.NopCloser(plaintextMaybeCompressed), nil // as-is
		case CompressionMethodDeflate:
			return flate.NewReader(plaintextMaybeCompressed), nil
		default:
			return nil, fmt.Errorf("unsupported CompressionMethod: %x", compressionMethod)
		}
	}()
	if err != nil {
		return nil, fmt.Errorf("Decrypt: %w", err)
	}

	// split plaintext into lines
	lines, err := scan(bufio.NewScanner(plaintext))
	if err != nil {
		return nil, fmt.Errorf("Decrypt: %w", err)
	}

	return lines, plaintext.Close()
}

func encryptStream(ciphertext io.Writer, plaintext io.Reader, cipherStream cipher.Stream) error {
	ciphertextWriter := cipher.StreamWriter{
		S: cipherStream,
		W: ciphertext,
	}
	if _, err := io.Copy(ciphertextWriter, plaintext); err != nil {
		return err
	}

	return ciphertextWriter.Close()
}

func scan(scanner *bufio.Scanner) ([]string, error) {
	items := []string{}
	for scanner.Scan() {
		items = append(items, scanner.Text())
	}

	return items, scanner.Err()
}
