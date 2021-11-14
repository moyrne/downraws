package drixlsx

import (
	"io"

	"github.com/moyrne/downraws"
	"github.com/pkg/errors"
	"github.com/xuri/excelize/v2"
)

func init() {
	downraws.Drivers["xlsx"] = Excel{}
}

type Excel struct{}

func (e Excel) NewWriter() downraws.Writer {
	f := excelize.NewFile()
	s, err := f.NewStreamWriter("Sheet1")
	if err != nil {
		// Unknown error
		panic(err)
	}
	return &ExcelWriter{writer: f, streamWriter: s, raw: 1}
}

func (e Excel) Suffix() string {
	return ".xlsx"
}

type ExcelWriter struct {
	writer       *excelize.File
	streamWriter *excelize.StreamWriter
	raw          int
}

func (e *ExcelWriter) Write(values ...interface{}) error {
	cell, err := excelize.CoordinatesToCellName(1, e.raw)
	if err != nil {
		return errors.WithStack(err)
	}
	e.raw++
	return errors.WithStack(e.streamWriter.SetRow(cell, values))
}

func (e *ExcelWriter) WriteTo(out io.Writer) (int64, error) {
	if err := e.streamWriter.Flush(); err != nil {
		return 0, errors.WithStack(err)
	}

	n, err := e.writer.WriteTo(out)
	return n, errors.WithStack(err)
}
