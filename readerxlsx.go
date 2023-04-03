package main

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/xuri/excelize/v2"
)

type XlsxReader struct {
	streamReader *excelize.File
	rows         *excelize.Rows
	header       []string
	options      *Options
}

func NewXlsxReader(r io.Reader, options *Options) (*XlsxReader, error) {
	return newXlsxReader(r, options)
}

func newXlsxReader(r io.Reader, options *Options) (*XlsxReader, error) {
	streamReader, err := excelize.OpenReader(r)
	if err != nil {
		return nil, fmt.Errorf("open reader: %w", err)
	}

	sheetName := options.sheetName
	if sheetName == "" {
		if len(streamReader.GetSheetList()) == 0 {
			streamReader.Close()
			return nil, fmt.Errorf("get sheet list: %w", errors.New("file does not contains any sheets"))
		}
		sheetName = streamReader.GetSheetList()[0]
	}

	rows, err := streamReader.Rows(sheetName)
	if err != nil {
		streamReader.Close()
		return nil, fmt.Errorf("read rows: %w", err)
	}

	xlsxReader := &XlsxReader{
		streamReader: streamReader,
		options:      options,
		rows:         rows}

	for i := 0; i < options.skipRows; i++ {
		_, err := xlsxReader.GetRow(true)
		if err != nil {
			streamReader.Close()
			return nil, fmt.Errorf("skip rows: %w", err)
		}
	}

	header, err := getHeader(xlsxReader)
	if err != nil {
		streamReader.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}
	xlsxReader.header = header

	return xlsxReader, nil

}

func (r *XlsxReader) GetHeader() []string {
	return r.header
}

func (r *XlsxReader) Options() *Options {
	return r.options
}

func (r *XlsxReader) GetRow(asStrings bool) ([]any, error) {
	end := !r.rows.Next()
	if end {
		return nil, io.EOF
	}

	record, err := r.rows.Columns(excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, err
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

func (r *XlsxReader) Close() error {
	err := r.rows.Close()
	if err != nil {
		return err
	}

	err = r.streamReader.Close()
	if err != nil {
		return err
	}

	return nil
}

func (r *XlsxReader) ParseDate(rawValue string) (time.Time, error) {
	f, err := strconv.ParseFloat(rawValue, 64)
	if err != nil {
		return time.Time{}, err
	}

	t, err := excelize.ExcelDateToTime(f, false)
	if err != nil {
		return time.Time{}, err
	}

	t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), r.options.timezone)

	return t, nil
}

func (r *XlsxReader) ParseDateTime(rawValue string) (time.Time, error) {
	f, err := strconv.ParseFloat(rawValue, 64)
	if err != nil {
		return time.Time{}, err
	}

	t, err := excelize.ExcelDateToTime(f, false)
	if err != nil {
		return time.Time{}, err
	}

	t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), r.options.timezone)

	return t, nil
}
