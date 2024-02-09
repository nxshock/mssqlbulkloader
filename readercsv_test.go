package main

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCsvReaderBasic(t *testing.T) {
	f, err := os.Open("testdata/csv/9729337841_20032023_084313667.csv")
	assert.NoError(t, err)

	options := &Options{
		encoding:        "win1251",
		comma:           rune(";"[0]),
		skipRows:        3,
		fieldsTypes:     "s     ttmmsssss",
		dateFormat:      "02.01.2006",
		timestampFormat: "02.01.2006 15:04:05",
		timezone:        time.Local}

	csvReader, err := NewCsvReader(f, options)
	assert.NoError(t, err)

	assert.Equal(t, []string{"RRN", "Дата операции", "Дата ПП", "Сумма операции", "Сумма расчета", "Номер карты", "Код авторизации", "Тип операции", "Доп. информация_1", "Доп. информация_2"}, csvReader.GetHeader())

	row, err := csvReader.GetRow(false)
	assert.NoError(t, err)

	t1 := time.Date(2023, 03, 19, 17, 49, 35, 0, time.Local)
	t2 := time.Date(2023, 03, 20, 0, 0, 0, 0, time.Local)
	assert.Equal(t, []any{"307814009186", t1, t2, 499.00, 488.52, "522598******7141", "REZE64", "Покупка", "35068281112", "307817403283"}, row)

	_, err = csvReader.GetRow(false)
	assert.Equal(t, err, io.EOF)

	err = csvReader.Close()
	assert.NoError(t, err)
}
