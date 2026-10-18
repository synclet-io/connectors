package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
)

var (
	gzipMagic = []byte{0x1f, 0x8b}
	zstdMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}
)

// detectCompression resolves auto: the key's extension decides, then the
// first bytes of the content.
func detectCompression(setting, key string, head []byte) string {
	if setting != compressionAuto {
		return setting
	}
	lower := strings.ToLower(key)
	switch {
	case strings.HasSuffix(lower, ".gz"), strings.HasSuffix(lower, ".gzip"):
		return compressionGzip
	case strings.HasSuffix(lower, ".zst"), strings.HasSuffix(lower, ".zstd"):
		return compressionZstd
	case bytes.HasPrefix(head, gzipMagic):
		return compressionGzip
	case bytes.HasPrefix(head, zstdMagic):
		return compressionZstd
	}
	return compressionNone
}

// decompress wraps r in the decompressor the stream's setting and the
// object call for. Closing the result releases the decompressor, not r.
func decompress(r io.Reader, setting, key string) (io.ReadCloser, error) {
	br := bufio.NewReader(r)
	head, err := br.Peek(len(zstdMagic))
	if err != nil && err != io.EOF {
		return nil, err
	}
	switch detectCompression(setting, key, head) {
	case compressionGzip:
		zr, err := gzip.NewReader(br)
		if err != nil {
			return nil, fmt.Errorf("gzip: %w", err)
		}
		return zr, nil
	case compressionZstd:
		zr, err := zstd.NewReader(br)
		if err != nil {
			return nil, fmt.Errorf("zstd: %w", err)
		}
		return zr.IOReadCloser(), nil
	}
	return io.NopCloser(br), nil
}
