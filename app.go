package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"time"
	_ "time/tzdata"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/urfave/cli"
)

var app = &cli.App{
	Version:  "2023.03.27",
	Usage:    "bulk loader into Microsoft SQL Server",
	HideHelp: true,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:      "filepath",
			Usage:     "input file path",
			Required:  true,
			TakesFile: true},
		&cli.StringFlag{
			Name:     "type",
			Usage:    "input file type",
			Required: false,
			Value:    "auto",
		},
		&cli.StringFlag{
			Name:     "encoding",
			Usage:    "input file encoding",
			Required: false,
			Value:    "utf8",
		},
		&cli.StringFlag{
			Name:     "sheetname",
			Usage:    "Excel file sheet name",
			Required: false},
		&cli.StringFlag{
			Name:  "server",
			Usage: "database server address",
			Value: "127.0.0.1"},
		&cli.StringFlag{
			Name:     "database",
			Usage:    "database name",
			Required: true},
		&cli.StringFlag{
			Name:     "table",
			Usage:    "table name in schema.name format",
			Required: true},
		&cli.StringFlag{
			Name:     "fields",
			Usage:    "list of field types in [sifdt ]+ format",
			Required: true},
		&cli.BoolFlag{
			Name:  "create",
			Usage: "create table"},
		&cli.BoolFlag{
			Name:  "overwrite",
			Usage: "overwrite existing table"},
		&cli.IntFlag{
			Name:  "skiprows",
			Usage: "number of rows to skip before read header"},
		&cli.BoolFlag{
			Name:  "unknowncolumnnames",
			Usage: "insert to table with unknown column names",
		},
		&cli.StringFlag{
			Name:  "timezone",
			Usage: "Time zone (IANA Time Zone database format)",
			Value: "Local",
		},
		&cli.StringFlag{
			Name:  "comma",
			Usage: "CSV file delimiter",
			Value: ",",
		},
		&cli.StringFlag{
			Name:  "dateformat",
			Usage: "date format (Go style)",
			Value: "02.01.2006"},
		&cli.StringFlag{
			Name:  "timestampformat",
			Usage: "timestamp format (Go style)",
			Value: "02.01.2006 15:04:05"},
		&cli.StringFlag{
			Name:  "decompress",
			Usage: "decompressor name for archived files",
		},
		&cli.BoolFlag{
			Name:  "silent",
			Usage: "disable output",
		},
	},
	Action: func(c *cli.Context) error {
		initLogger(c.Bool("silent"))

		var comma rune
		if c.String("comma") == "\\t" {
			comma = rune("\t"[0])
		} else {
			comma = rune(c.String("comma")[0])
		}

		location, err := time.LoadLocation(c.String("timezone"))
		if err != nil {
			return fmt.Errorf("parse timezone: %w", err)
		}

		options := &Options{
			filePath:           c.String("filepath"),
			fileType:           c.String("type"),
			sheetName:          c.String("sheetname"),
			server:             c.String("server"),
			database:           c.String("database"),
			tableName:          c.String("table"),
			fieldsTypes:        c.String("fields"),
			create:             c.Bool("create"),
			overwrite:          c.Bool("overwrite"),
			skipRows:           c.Int("skiprows"),
			encoding:           c.String("encoding"),
			dateFormat:         c.String("dateformat"),
			timestampFormat:    c.String("timestampformat"),
			timezone:           location,
			decompress:         c.String("decompress"),
			unknownColumnNames: c.Bool("unknowncolumnnames"),
			silent:             c.Bool("silent"),
			comma:              comma,
		}

		if options.decompress != "" {
			var archiveType ArchiveType
			err = archiveType.UnmarshalText([]byte(options.decompress))
			if err != nil {
				return err
			}

			ar, err := archiveType.Open()
			if err != nil {
				return err
			}

			err = ar.Process(options)
			if err != nil {
				return err
			}
		} else {
			f, err := os.Open(options.filePath)
			if err != nil {
				return err
			}
			defer f.Close()

			err = process(f, options)
			if err != nil {
				return err
			}
		}

		logger.Print("Complete.")

		return nil
	}}

func process(r io.Reader, options *Options) error {
	var fileType FileType
	err := fileType.UnmarshalText([]byte(options.fileType))
	if err != nil {
		return err
	}

	reader, err := fileType.Open(r, options)
	if err != nil {
		return err
	}
	defer reader.Close()

	db, err := sql.Open("sqlserver", fmt.Sprintf("sqlserver://%s?database=%s", options.server, options.database))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	err = prepareTable(reader, tx)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare table: %w", err)
	}

	err = insertData(reader, tx)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("insert data: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
