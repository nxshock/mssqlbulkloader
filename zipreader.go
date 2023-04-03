package main

import (
	"archive/zip"
	"io"
)

type ProcessFunc func(io.Reader, *Options) error

type ZipReader struct{}

func (zr *ZipReader) Process(options *Options) error {
	z, err := zip.OpenReader(options.filePath)
	if err != nil {
		return err
	}
	defer z.Close()

	for _, zFile := range z.File {
		f, err := zFile.Open()
		if err != nil {
			return err
		}
		defer f.Close()

		err = process(f, options)
		if err != nil {
			return err
		}
	}

	return nil
}
