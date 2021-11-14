package downraws_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"strconv"
	"testing"

	_ "github.com/moyrne/downraws/drivers/dricsv"
	_ "github.com/moyrne/downraws/drivers/drixlsx"

	"github.com/moyrne/downraws"
	"github.com/stretchr/testify/assert"
)

func TestDownRaws(t *testing.T) {
	testData = make([][]interface{}, 0, 8000)
	for i := 0; i < 8000; i++ {
		testData = append(testData, []interface{}{
			strconv.Itoa(i), strconv.Itoa(i), strconv.Itoa(i), strconv.Itoa(i), strconv.Itoa(i),
		})
	}

	r, err := downraws.New("csv", "test_csv", testDownRawsFn)
	assert.Equal(t, nil, err)

	ctx := context.Background()
	err = r.LoadData(ctx)
	assert.Equal(t, nil, err)

	buf := bytes.NewBuffer(nil)
	_, err = r.WriteTo(buf)
	assert.Equal(t, nil, err)

	z, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	assert.Equal(t, nil, err)

	f1, err := z.Open("test_csv_1.csv")
	assert.Equal(t, nil, err)
	c1, err := csv.NewReader(f1).ReadAll()
	assert.Equal(t, nil, err)
	ce1, err := testDownRawsFn(ctx, 3000, 0)
	assert.Equal(t, nil, err)
	assert.JSONEq(t, jsonMarshalOrDie(ce1), jsonMarshalOrDie(c1))

	f2, err := z.Open("test_csv_2.csv")
	assert.Equal(t, nil, err)
	c2, err := csv.NewReader(f2).ReadAll()
	assert.Equal(t, nil, err)
	ce2, err := testDownRawsFn(ctx, 3000, 3000)
	assert.Equal(t, nil, err)
	assert.JSONEq(t, jsonMarshalOrDie(ce2), jsonMarshalOrDie(c2))

	f3, err := z.Open("test_csv_3.csv")
	assert.Equal(t, nil, err)
	c3, err := csv.NewReader(f3).ReadAll()
	assert.Equal(t, nil, err)
	ce3, err := testDownRawsFn(ctx, 3000, 6000)
	assert.Equal(t, nil, err)
	assert.JSONEq(t, jsonMarshalOrDie(ce3), jsonMarshalOrDie(c3))
}

var testData [][]interface{}

func testDownRawsFn(ctx context.Context, limit, offset int) ([][]interface{}, error) {
	if offset >= len(testData) {
		return nil, nil
	}
	res := make([][]interface{}, 0, limit)
	for i := offset; i < offset+limit; i++ {
		if i >= len(testData) {
			break
		}
		res = append(res, testData[i])
	}
	return res, nil
}

func jsonMarshalOrDie(v interface{}) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(data)
}
