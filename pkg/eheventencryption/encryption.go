// Encrypts (& maybe compresses) line-based (\n) data (usually "ehevent" serialization format) into a LogData item
package eheventencryption

import (
	"bytes"
	"compress/flate"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"golang.org/x/crypto/chacha20poly1305"
)

// format is: <reserved 0x0> <compression method> <dek version> <iv> <ciphertextMaybeCompressed>
//
// 1 byte     Header
//            |- upper 4 bits reserved. if non-zero, assume incompatible format version & stop decoding!
//            └- lowest 4 bits compression method (0x0 = uncompressed, 0x1 = deflate)
// 1-n bytes  Reserved for DEK version (key rotation). N is defined by encoding/binary.Uvarint semantics
// 16 bytes   IV
// rest       chacha20poly1305(plaintextMaybeCompressed, dek).
//
// plaintext is multiple "ehevent" lines split by \n character.

// AES256-GCM was considered for the AEAD due to its > 5x hardware-accelerated performance[^1] compared
// to ChaCha20-Poly1305, but considering critique[^2] on it and WireGuard's[^3] (& age's) embrace,
// ChaCha20-Poly1305 was selected.
//
// [^1]: https://www.bearssl.org/speed.html
// [^2]: https://soatok.blog/2020/05/13/why-aes-gcm-sucks/
// [^3]: https://www.wireguard.com/protocol/

// Not using XChaCha20 because a single stream isn't expected to get > 2^32 log messages using the
// same stream-specific key version (we have key rotation)

type CompressionMethod byte

const (
	CompressionMethodNone    CompressionMethod = 0
	CompressionMethodDeflate CompressionMethod = 1
)

func Encrypt(plaintext []byte, dek []byte) ([]byte, error) {
	return encryptWithRand(plaintext, dek, rand.Reader)
}

func encryptWithRand(plaintext []byte, dek []byte, cryptoRand io.Reader) ([]byte, error) {
	if len(plaintext) == 0 {
		return nil, errors.New("Encrypt: no data")
	}

	plaintextMaybeCompressed, compressionMethod, err := compressIfWellCompressible(
		plaintext)
	if err != nil {
		return nil, err
	}

	aead, err := chacha20poly1305.New(dek)
	if err != nil {
		return nil, err
	}

	// header's upper 4 bits are currently zero (and compressionMethod uses lower 4 bits),
	// so our header is basically compressionMethod
	header := byte(compressionMethod)

	// the 0x00 for "reserved" semantics amount to high bit=varuint fits in 1 byte, and next
	// 7 bits being zero signals that we use DEK v0 (i.e. this is not a rotated DEK)
	dekVersion := byte(0x00)

	// ___________  header ______________
	// <reserved 0x0> <compressionMethod> <DEK version> <nonce> <ciphertext> <authtag>
	buffer := make([]byte, 2+aead.NonceSize()+len(plaintextMaybeCompressed)+aead.Overhead())
	buffer[0] = header
	buffer[1] = dekVersion // TODO: when implemented, length of this is dynamic

	nonce := buffer[2 : 2+aead.NonceSize()]

	if _, err := io.ReadFull(cryptoRand, nonce); err != nil {
		return nil, err
	}

	aead.Seal(buffer[2:2+aead.NonceSize()], nonce, plaintextMaybeCompressed, nil)

	return buffer, nil
}

func Decrypt(ciphertext []byte, dek []byte) ([]byte, error) {
	raw := bytes.NewReader(ciphertext)

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

	// after reading nonce, next reads contain only ciphertext
	nonce := make([]byte, chacha20poly1305.NonceSize)
	if _, err := io.ReadFull(raw, nonce); err != nil {
		return nil, fmt.Errorf("Decrypt: read nonce: %w", err)
	}

	ciphertextAndAuthTag, err := io.ReadAll(raw)
	if err != nil {
		return nil, err
	}

	aead, err := chacha20poly1305.New(dek)
	if err != nil {
		return nil, err
	}

	plaintextMaybeCompressed, err := aead.Open(ciphertextAndAuthTag[:0], nonce, ciphertextAndAuthTag, nil)
	if err != nil {
		return nil, err
	}

	plaintextReader, err := func() (io.ReadCloser, error) {
		switch compressionMethod {
		case CompressionMethodNone:
			return ioutil.NopCloser(bytes.NewReader(plaintextMaybeCompressed)), nil // as-is
		case CompressionMethodDeflate:
			return flate.NewReader(bytes.NewReader(plaintextMaybeCompressed)), nil
		default:
			return nil, fmt.Errorf("unsupported CompressionMethod: %x", compressionMethod)
		}
	}()
	if err != nil {
		return nil, fmt.Errorf("Decrypt: %w", err)
	}

	plaintext, err := io.ReadAll(plaintextReader)
	if err != nil {
		return nil, fmt.Errorf("Decrypt: %w", err)
	}

	if err := plaintextReader.Close(); err != nil {
		return nil, fmt.Errorf("Decrypt: %w", err)
	}

	return plaintext, nil
}
