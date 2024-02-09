package main

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDbfReaderBasic(t *testing.T) {
	f, err := os.Open("testdata/dbf/38_052QB.dbf")
	assert.NoError(t, err)

	options := &Options{
		fieldsTypes: "sssssstdmmsss",
		timezone:    time.Local,
		encoding:    "cp866"}

	dbfReader, err := NewDbfReader(f, options)
	assert.NoError(t, err)

	assert.Equal(t, []string{"TRAN_ID", "БАНК", "ОТДЕЛЕНИЕ", "ТОЧКА", "НАЗВАНИЕ", "ТЕРМИНАЛ", "ДАТА_ТРАН", "ДАТА_РАСЧ", "СУММА_ТРАН", "СУММА_РАСЧ", "КАРТА", "КОД_АВТ", "ТИП"}, dbfReader.GetHeader())

	row, err := dbfReader.GetRow(false)
	assert.NoError(t, err)

	t1 := time.Date(2023, 02, 20, 5, 57, 12, 0, time.Local)
	t2 := time.Date(2023, 02, 21, 0, 0, 0, 0, time.Local)
	assert.Equal(t, []any{"719089383780", "44", "8644", "570000009312", "STOLOVAYA TSPP", "844417", t1, t2, 1757.08, 1713.15, "536829XXXXXX9388", "UM1TS8", "D"}, row)

	_, err = dbfReader.GetRow(false)
	assert.Equal(t, err, io.EOF)

	err = dbfReader.Close()
	assert.NoError(t, err)
}
