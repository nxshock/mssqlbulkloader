package main

import (
	"bytes"
	"fmt"
	"io"
	"time"

	dbf "github.com/SebastiaanKlippert/go-foxpro-dbf"
)

func init() {
	dbf.SetValidFileVersionFunc(func(version byte) error {
		return nil
	})
}

type DbfReader struct {
	reader  *dbf.DBF
	header  []string
	options *Options
}

func NewDbfReader(r io.Reader, options *Options) (*DbfReader, error) {
	return newDbfReader(r, options)
}

func newDbfReader(r io.Reader, options *Options) (*DbfReader, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	br := bytes.NewReader(b)

	re, err := dbf.OpenStream(br, nil, &dbf.UTF8Decoder{})
	if err != nil {
		return nil, err
	}

	dbfReader := &DbfReader{
		reader:  re,
		options: options}

	fullHeader := re.FieldNames()
	var header []string
	for i, v := range options.fieldsTypes {
		if v == ' ' {
			continue
		}

		s, err := charsets.DecodeString(options.encoding, fullHeader[i])
		if err != nil {
			return nil, err
		}

		header = append(header, s)
	}

	dbfReader.header = header

	return dbfReader, nil
}

func (r *DbfReader) GetHeader() []string {
	return r.header
}

func (r *DbfReader) Options() *Options {
	return r.options
}

func (r *DbfReader) GetRow(asStrings bool) ([]any, error) {
	if r.reader.EOF() {
		return nil, io.EOF
	}

	record, err := r.reader.Record()
	if err != nil {
		return nil, fmt.Errorf("read record: %v", err)
	}

	r.reader.Skip(1)

	var args []any

	for i, v := range record.FieldSlice() {
		var fieldType FieldType
		err = fieldType.UnmarshalText([]byte{r.options.fieldsTypes[i]})
		if err != nil {
			return nil, fmt.Errorf("get record type: %v", err)
		}

		if fieldType == Skip {
			continue
		}

		decV, err := charsets.DecodeString(r.options.encoding, fmt.Sprint(v))
		if err != nil {
			return nil, err
		}

		parsedValue, err := fieldType.ParseValue(r, decV)
		if err != nil {
			return nil, fmt.Errorf("parse value: %v", err)
		}

		args = append(args, parsedValue)
	}

	return args, nil
}

func (r *DbfReader) Close() error {
	return nil
}

func (r *DbfReader) ParseDate(rawValue string) (time.Time, error) {
	return time.ParseInLocation("02.01.2006", rawValue, r.options.timezone)
}

func (r *DbfReader) ParseDateTime(rawValue string) (time.Time, error) {
	return time.ParseInLocation("02.01.2006 15:04:05", rawValue, r.options.timezone)
}
