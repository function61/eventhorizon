package eh

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
)

// TODO: move to own package?

func MakeEncryptedData(events []string, dek []byte) (*LogData, error) {
	return makeEncryptedDataWithRand(events, dek, rand.Reader)
}

func makeEncryptedDataWithRand(events []string, dek []byte, cryptoRand io.Reader) (*LogData, error) {
	if len(events) == 0 {
		return nil, errors.New("EncryptedData: empty event slices are not supported")
	}

	plaintext := strings.Join(events, "\n")

	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(cryptoRand, iv); err != nil {
		return nil, err
	}

	aesCipher, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}

	ciphertext := &bytes.Buffer{}

	if err := encrypt(ciphertext, strings.NewReader(plaintext), cipher.NewCTR(aesCipher, iv)); err != nil {
		return nil, err
	}

	return &LogData{
		Kind: LogDataKindEncryptedData,
		Raw:  append(iv, ciphertext.Bytes()...),
	}, nil
}

func EncryptedDataDecrypt(data LogData, dek []byte) ([]string, error) {
	if data.Kind != LogDataKindEncryptedData {
		return nil, fmt.Errorf("EncryptedDataDecrypt: kind not LogDataKindEncryptedData; got %d", data.Kind)
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

	lines := bufio.NewScanner(&cipher.StreamReader{
		S: cipher.NewCTR(aesCipher, iv),
		R: ivAndCiphertext,
	})

	events := []string{}
	for lines.Scan() {
		events = append(events, lines.Text())
	}

	return events, lines.Err()
}

func encrypt(ciphertext io.Writer, plaintext io.Reader, cipherStream cipher.Stream) error {
	ciphertextWriter := cipher.StreamWriter{
		S: cipherStream,
		W: ciphertext,
	}
	if _, err := io.Copy(ciphertextWriter, plaintext); err != nil {
		return err
	}

	return ciphertextWriter.Close()
}
