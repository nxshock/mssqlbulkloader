package main

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestXlsxReaderBasic(t *testing.T) {
	f, err := os.Open("testdata/xlsx/38_049RMZ_all.xlsx")
	assert.NoError(t, err)

	options := &Options{skipRows: 0, fieldsTypes: "s sssssssssttfffssss", timezone: time.Local}

	xlsxReader, err := NewXlsxReader(f, options)
	assert.NoError(t, err)

	assert.Equal(t, []string{"ИНН предприятия", "Город", "Адрес ТСТ", "Обслуживающее отделение", "Расчетное отделение", "RRN операции", "Название ТСТ", "Мерчант ТСТ", "Расчетный мерчант", "Терминал", "Дата проведения операции", "Дата обработки операции", "Сумма операции", "Комиссия за операцию", "Сумма к расчету", "Карта", "Код авторизации", "Тип операции", "Тип карты"}, xlsxReader.GetHeader())

	row, err := xlsxReader.GetRow(false)
	assert.NoError(t, err)

	t1 := time.Date(2023, 02, 17, 1, 5, 12, 0, time.Local)
	t2 := time.Date(2023, 02, 18, 6, 24, 24, 0, time.Local) // TODO: в excel-файле 37 секунд?

	assert.Equal(t, []any{"7710146208", nil, nil, "99386901", "99386901", "304722813269", "TSENTRALNYY TELEGRAF", "780000334079", "780000334079", "10432641", t1, t2, 50.00, 0.80, 49.20, "553691******1214", "026094", "D", "MC OTHER"}, row)

	row, err = xlsxReader.GetRow(false)
	assert.NoError(t, err)
	assert.Len(t, row, 19)

	_, err = xlsxReader.GetRow(false)
	assert.Equal(t, io.EOF, err)

	err = xlsxReader.Close()
	assert.NoError(t, err)
}
