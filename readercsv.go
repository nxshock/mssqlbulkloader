package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
)

type CsvReader struct {
	reader  *csv.Reader
	header  []string
	options *Options
}

func NewCsvReader(r io.Reader, options *Options) (*CsvReader, error) {
	return newCsvReader(r, options)
}

func newCsvReader(r io.Reader, options *Options) (*CsvReader, error) {
	decoder, err := charsets.DecodeReader(options.encoding, r)
	if err != nil {
		return nil, fmt.Errorf("enable decoder: %v", options.encoding)
	}

	bufReader := bufio.NewReaderSize(decoder, 4*1024*1024)

	for i := 0; i < options.skipRows; i++ {
		_, _, err := bufReader.ReadLine()
		if err != nil {
			return nil, fmt.Errorf("skip rows: %v", err)
		}
	}

	re := csv.NewReader(bufReader)
	re.Comma = options.comma
	re.FieldsPerRecord = len(options.fieldsTypes)
	re.LazyQuotes = true

	csvReader := &CsvReader{
		reader:  re,
		options: options}

	header, err := getHeader(csvReader)
	if err != nil {
		return nil, err
	}

	csvReader.header = header

	return csvReader, nil

}

func (r *CsvReader) GetHeader() []string {
	return r.header
}

func (r *CsvReader) Options() *Options {
	return r.options
}

func (r *CsvReader) GetRow(asStrings bool) ([]any, error) {
	record, err := r.reader.Read()
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("read record: %v", err)
	}

	var args []any

	for i, v := range record {
		var fieldType FieldType
		err = fieldType.UnmarshalText([]byte{r.options.fieldsTypes[i]})
		if err != nil {
			return nil, fmt.Errorf("get record type: %v", err)
		}

		if fieldType == Skip {
			continue
		}

		if asStrings {
			fieldType = String
		}

		parsedValue, err := fieldType.ParseValue(r, v)
		if err != nil {
			return nil, fmt.Errorf("parse value: %v", err)
		}

		args = append(args, parsedValue)
	}

	return args, nil
}

func (r *CsvReader) Close() error {
	return nil
}
