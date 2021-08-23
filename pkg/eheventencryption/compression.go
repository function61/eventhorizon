package eheventencryption

import (
	"bytes"
	"compress/flate"
	"io"
)

// opportunistic compression: try compressing, and if it helped measurably, use it
func compressIfWellCompressible(input []byte) ([]byte, CompressionMethod, error) {
	compressed := &bytes.Buffer{}
	if err := compressDeflate(bytes.NewReader(input), compressed); err != nil {
		return nil, CompressionMethodNone, err
	}

	compressionRatio := float64(compressed.Len()) / float64(len(input))

	wellCompressible := compressionRatio < 0.9

	if wellCompressible {
		return compressed.Bytes(), CompressionMethodDeflate, nil
	} else {
		return input, CompressionMethodNone, nil
	}
}

func compressDeflate(input io.Reader, output io.Writer) error {
	compress, err := flate.NewWriter(output, flate.BestCompression)
	if err != nil {
		return err
	}
	if _, err := io.Copy(compress, input); err != nil {
		return err
	}

	return compress.Close()
}
