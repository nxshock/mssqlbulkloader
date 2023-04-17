package main

import (
	"fmt"
)

type ArchiveType int

const (
	AutoDetectArchiveType ArchiveType = iota
	Zip
	Zstd
)

type ArchiveProcessor interface {
	Process(options *Options) error
}

func (ft ArchiveType) MarshalText() (text []byte, err error) {
	switch ft {
	case AutoDetectArchiveType:
		return []byte("auto"), nil
	case Zip:
		return []byte("zip"), nil
	case Zstd:
		return []byte("zstd"), nil
	}

	return nil, fmt.Errorf("unknown type id = %d", ft)
}

func (ft ArchiveType) Open() (ArchiveProcessor, error) {
	switch ft {
	case AutoDetectArchiveType:
	case Zip:
		return new(ZipReader), nil
	case Zstd:
		return new(ZstdReader), nil
	}

	return nil, fmt.Errorf("unknown type id = %d", ft)
}

func (ft *ArchiveType) UnmarshalText(text []byte) error {
	switch string(text) {
	case "auto":
		*ft = AutoDetectArchiveType
		return nil
	case "zip":
		*ft = Zip
		return nil
	case "zstd":
		*ft = Zstd
		return nil
	}

	return fmt.Errorf(`unknown format code "%s"`, string(text))
}
