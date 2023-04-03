package main

import (
	"fmt"
	"io"
)

type FileType int

const (
	AutoDetect FileType = iota
	Csv
	Xlsx
	Dbf
)

func (ft FileType) MarshalText() (text []byte, err error) {
	switch ft {
	case AutoDetect:
		return []byte("auto"), nil
	case Csv:
		return []byte("csv"), nil
	case Xlsx:
		return []byte("xlsx"), nil
	case Dbf:
		return []byte("dbf"), nil
	}

	return nil, fmt.Errorf("unknown type id = %d", ft)
}

func (ft FileType) Open(r io.Reader, options *Options) (Reader, error) {
	switch ft {
	case AutoDetect:
	case Csv:
		return newCsvReader(r, options)
	case Xlsx:
		return newXlsxReader(r, options)
	case Dbf:
		return newDbfReader(r, options)
	}

	return nil, fmt.Errorf("unknown type id = %d", ft)
}

func (ft *FileType) UnmarshalText(text []byte) error {
	switch string(text) {
	case "auto":
		*ft = AutoDetect
		return nil
	case "csv":
		*ft = Csv
		return nil
	case "xlsx":
		*ft = Xlsx
		return nil
	case "dbf":
		*ft = Dbf
		return nil
	}

	return fmt.Errorf(`unknown format code "%s"`, string(text))
}
