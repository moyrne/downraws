package dricsv

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/moyrne/downraws"
	"github.com/pkg/errors"
)

func init() {
	downraws.Drivers["csv"] = CSV{}
}

type CSV struct{}

func (c CSV) NewWriter() downraws.Writer {
	buffer := bytes.NewBuffer(nil)
	return &CSVWriter{writer: csv.NewWriter(buffer), buffer: buffer}
}

func (c CSV) Suffix() string {
	return ".csv"
}

type CSVWriter struct {
	buffer *bytes.Buffer
	writer *csv.Writer
}

func (c *CSVWriter) Write(values ...interface{}) error {
	vs := make([]string, 0, len(values))
	for i := 0; i < len(values); i++ {
		vs = append(vs, fmt.Sprintf("%v", values[i]))
	}
	return errors.WithStack(c.writer.Write(vs))
}

func (c *CSVWriter) WriteTo(out io.Writer) (int64, error) {
	c.writer.Flush()
	n, err := io.Copy(out, c.buffer)
	return n, errors.WithStack(err)
}
