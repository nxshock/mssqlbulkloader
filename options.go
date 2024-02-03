package main

import "time"

type Options struct {
	// Source file path
	filePath string

	// Source file type
	fileType string

	// Server address
	server string

	// Database name
	database string

	// Table name
	tableName string

	// comma delimiter for CSV files
	comma rune

	// Number of rows to skip before reading of header
	skipRows int

	// List of fiels types
	fieldsTypes string

	// Date format
	dateFormat string

	// Date+time format
	timestampFormat string

	// Sheet name for Excel file
	sheetName string

	// CSV/DBF codepage
	encoding string

	// create table before inserting data
	create bool

	// Drop existing table before creating
	overwrite bool

	// Disable progress output
	silent bool

	// Input file dates timezone
	timezone *time.Location

	// Decompress before process
	decompress string

	// Unknown column names
	unknownColumnNames bool

	// Column names list
	columnNames []string
}

func (o *Options) fieldCount() int {
	fCount := 0
	for i := range o.fieldsTypes {
		if o.fieldsTypes[i] != ' ' {
			fCount++
		}
	}

	return fCount
}
