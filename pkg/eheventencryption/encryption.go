// Encrypts line-based data (usually "ehevent" serialization format) into a LogData item
package eheventencryption

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/function61/eventhorizon/pkg/eh"
)

func Encrypt(lines []string, dek []byte) (*eh.LogData, error) {
	return encryptWithRand(lines, dek, rand.Reader)
}

func encryptWithRand(lines []string, dek []byte, cryptoRand io.Reader) (*eh.LogData, error) {
	if len(lines) == 0 {
		return nil, errors.New("EncryptedData: empty event slices are not supported")
	}

	plaintext := strings.Join(lines, "\n")

	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(cryptoRand, iv); err != nil {
		return nil, err
	}

	aesCipher, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}

	ciphertext := &bytes.Buffer{}

	if err := encryptStream(ciphertext, strings.NewReader(plaintext), cipher.NewCTR(aesCipher, iv)); err != nil {
		return nil, err
	}

	return &eh.LogData{
		Kind: eh.LogDataKindEncryptedData,
		Raw:  append(iv, ciphertext.Bytes()...),
	}, nil
}

func Decrypt(data eh.LogData, dek []byte) ([]string, error) {
	if data.Kind != eh.LogDataKindEncryptedData {
		return nil, fmt.Errorf("Decrypt: kind not LogDataKindEncryptedData; got %d", data.Kind)
	}

	ivAndCiphertext := bytes.NewReader(data.Raw)

	// after reading IV, next reads contain only ciphertext
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(ivAndCiphertext, iv); err != nil {
		return nil, err
	}

	aesCipher, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}

	// split plaintext into lines
	return scan(bufio.NewScanner(&cipher.StreamReader{
		S: cipher.NewCTR(aesCipher, iv),
		R: ivAndCiphertext,
	}))
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
