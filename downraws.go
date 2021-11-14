package downraws

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"strconv"

	"github.com/pkg/errors"
)

type DownRaws struct {
	downFunc func(ctx context.Context, limit, offset int) ([][]interface{}, error)
	fields   []string

	// Prohibit limit greater than maxRaw
	limit  int
	offset int
	maxRaw int

	driver Driver

	filename  string
	zipBuffer *bytes.Buffer
	zipWriter *zip.Writer
}

var (
	ErrDriverIsNotRegister            = errors.New("driver is not register")
	ErrProhibitLimitGreaterThanMaxRaw = errors.New("Prohibit limit greater than maxRaw")
)

func New(driverName, filename string, fn func(ctx context.Context, limit, offset int) ([][]interface{}, error), options ...Option) (*DownRaws, error) {
	driver, ok := Drivers[driverName]
	if !ok {
		return nil, errors.Wrapf(ErrDriverIsNotRegister, "driver name: %s", driverName)
	}

	zipBuffer := bytes.NewBuffer(nil)
	dr := &DownRaws{
		downFunc:  fn,
		fields:    nil,
		limit:     3000,
		offset:    0,
		maxRaw:    3000,
		driver:    driver,
		filename:  filename,
		zipBuffer: zipBuffer,
		zipWriter: zip.NewWriter(zipBuffer),
	}

	for _, option := range options {
		option(dr)
	}

	if dr.limit > dr.maxRaw {
		_ = dr.zipWriter.Close()
		return nil, errors.Wrapf(ErrProhibitLimitGreaterThanMaxRaw, "limit: %d; maxRaw: %d", dr.limit, dr.maxRaw)
	}

	return dr, nil
}

func (r *DownRaws) LoadData(ctx context.Context) error {
	return r.safeClose(func() error {
		var (
			idx        int
			writeCount int
			writer     = r.driver.NewWriter()
		)
		for ; ; r.offset += r.limit {
			if ctx.Err() != nil {
				return errors.WithStack(ctx.Err())
			}

			data, err := r.downFunc(ctx, r.limit, r.offset)
			if err != nil {
				return err
			}
			if len(data) == 0 {
				break
			}

			for i := 0; i < len(data); i++ {
				if writeCount >= r.maxRaw {
					idx++
					f, err := r.zipWriter.Create(r.filename + "_" + strconv.Itoa(idx) + r.driver.Suffix())
					if err != nil {
						return errors.WithStack(err)
					}
					if _, err := writer.WriteTo(f); err != nil {
						return err
					}
					writer = r.driver.NewWriter()
					writeCount = 0
				}
				if err := writer.Write(data[i]...); err != nil {
					return err
				}
				writeCount++
			}
		}

		if writeCount != 0 {
			idx++
			f, err := r.zipWriter.Create(r.filename + "_" + strconv.Itoa(idx) + r.driver.Suffix())
			if err != nil {
				return errors.WithStack(err)
			}
			if _, err := writer.WriteTo(f); err != nil {
				return err
			}
		}

		if err := r.zipWriter.Flush(); err != nil {
			return errors.WithStack(err)
		}

		return errors.WithStack(r.zipWriter.Close())
	})
}

func (r *DownRaws) safeClose(fn func() error) error {
	if err := fn(); err != nil {
		if e := r.zipWriter.Close(); e != nil {
			return errors.WithMessagef(err, "close failed: %v", e)
		}
		return err
	}
	return nil
}

func (r *DownRaws) WriteTo(writer io.Writer) (int64, error) {
	return r.zipBuffer.WriteTo(writer)
}

type Option func(r *DownRaws)

func SetFields(fields ...string) Option {
	return func(r *DownRaws) {
		r.fields = fields
	}
}

func SetLimit(limit int) Option {
	return func(r *DownRaws) {
		r.limit = limit
	}
}

func SetOffset(offset int) Option {
	return func(r *DownRaws) {
		r.offset = offset
	}
}

func SetMaxRaw(maxRaw int) Option {
	return func(r *DownRaws) {
		r.maxRaw = maxRaw
	}
}
