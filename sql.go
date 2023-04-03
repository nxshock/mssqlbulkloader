package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"

	mssql "github.com/denisenkom/go-mssqldb"
)

// TODO: add escaping
func prepareTable(reader Reader, tx *sql.Tx) error {
	if reader.Options().unknownColumnNames {
		var columnNames []string

		sql := fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA + '.' + TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION", reader.Options().tableName)
		rows, err := tx.Query(sql)
		if err != nil {
			return fmt.Errorf("get column names from database: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			if rows.Err() != nil {
				return fmt.Errorf("get column names from database: %w", err)
			}
			var columnName string
			err = rows.Scan(&columnName)
			if err != nil {
				return fmt.Errorf("get column names from database: %w", err)
			}
			columnNames = append(columnNames, columnName)
		}

		reader.Options().columnNames = columnNames
	} else {
		reader.Options().columnNames = reader.GetHeader()
	}

	if !reader.Options().create && !reader.Options().overwrite {
		return nil
	}

	if !reader.Options().create && reader.Options().overwrite {
		logger.Println("Truncating table...")
		_, err := tx.Exec(fmt.Sprintf("TRUNCATE TABLE %s", reader.Options().tableName))
		if err != nil {
			return err
		}
	}

	if reader.Options().overwrite {
		logger.Println("Dropping table...")
		_, err := tx.Exec(fmt.Sprintf("IF object_id('%s', 'U') IS NOT NULL DROP TABLE %s", reader.Options().tableName, reader.Options().tableName))
		if err != nil {
			return fmt.Errorf("drop table: %w", err)
		}
	}

	sql := fmt.Sprintf("CREATE TABLE %s (", reader.Options().tableName)

	fieldTypes := strings.ReplaceAll(reader.Options().fieldsTypes, " ", "")

	for i, columnName := range reader.Options().columnNames {
		var fieldType FieldType
		err := fieldType.UnmarshalText([]byte{fieldTypes[i]})
		if err != nil {
			return fmt.Errorf("detect field type: %w", err)
		}

		sql += fmt.Sprintf(`"%s" %s`, columnName, fieldType.SqlFieldType())

		if i+1 < len(reader.GetHeader()) {
			sql += ", "
		} else {
			sql += ") WITH (DATA_COMPRESSION = PAGE)" // TODO: add optional params
		}
	}

	logger.Println("Creating table...")
	logger.Println(sql)
	_, err := tx.Exec(sql)
	if err != nil {
		return fmt.Errorf("execute table creation: %w", err)
	}

	return nil
}

func insertData(reader Reader, tx *sql.Tx) error {
	columnNames := reader.GetHeader()
	if reader.Options().unknownColumnNames {
		columnNames = reader.Options().columnNames
	}

	sql := mssql.CopyIn(reader.Options().tableName, mssql.BulkOptions{Tablock: true}, columnNames...)

	stmt, err := tx.Prepare(sql)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare statement: %w", err)
	}

	n := 0
	for {
		if n%100000 == 0 {
			if !reader.Options().silent {
				fmt.Fprintf(os.Stderr, "Processed %d records...\r", n)
			}
		}

		record, err := reader.GetRow(false)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read record: %w", err)
		}

		_, err = stmt.Exec(record...)
		if err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return fmt.Errorf("execute statement: %w", err)
		}
		n++
	}
	result, err := stmt.Exec()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("execute statement: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("calc rows affected: %w", err)
	}
	if !reader.Options().silent {
		fmt.Fprintf(os.Stderr, "Processed %d records.  \n", rowsAffected)
	}

	err = stmt.Close()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("close statement: %w", err)
	}

	return nil
}
