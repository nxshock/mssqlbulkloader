package main

import (
	"fmt"
	"io"

	"github.com/dimchansky/utfbom"
	"golang.org/x/text/encoding/charmap"
)

type Charset interface {
	String(string) (string, error)
	Reader(io.Reader) io.Reader
}

type Charsets map[string]Charset

var charsets = make(Charsets)

func (c Charsets) Register(name string, charset Charset) {
	c[name] = charset
}

func (c Charsets) DecodeString(name string, input string) (string, error) {
	decoder, ok := c[name]
	if !ok {
		return "", fmt.Errorf("unknown decoder: %s", name)
	}

	if decoder == nil {
		return input, nil
	}

	return decoder.String(input)
}

func (c Charsets) DecodeReader(name string, input io.Reader) (io.Reader, error) {
	decoder, ok := charsets[name]
	if !ok {
		return nil, fmt.Errorf("unknown decoder: %s", name)
	}

	if decoder == nil {
		return input, nil
	}

	return decoder.Reader(input), nil
}

func init() {
	charsets.Register("utf8", utf8decoder)
	charsets.Register("win1251", charmap.Windows1251.NewDecoder())
	charsets.Register("cp866", charmap.CodePage866.NewDecoder())
}

type Utf8decoder struct{}

var utf8decoder = new(Utf8decoder)

func (d *Utf8decoder) Reader(r io.Reader) io.Reader {
	return utfbom.SkipOnly(r)
}

func (d *Utf8decoder) String(s string) (string, error) {
	return s, nil
}
