package main

import (
	"os"

	"github.com/klauspost/compress/zstd"
)

type ZstdReader struct{}

func (zr *ZstdReader) Process(options *Options) error {
	f, err := os.Open(options.filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := zstd.NewReader(f)
	if err != nil {
		return err
	}
	defer r.Close()

	return process(f, options)
}
