package ingest

import (
	"bufio"
	"encoding/csv"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

// ConvertCSVToParquet converts a CSV file to a Parquet file (strings only for simplicity).
func ConvertCSVToParquet(csvPath, outParquetPath string) error {
	f, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReader(f))
	// read header
	header, err := r.Read()
	if err != nil {
		return err
	}

	// wrap the file path using LocalFileWriter
	fw, err := local.NewLocalFileWriter(outParquetPath)
	if err != nil {
		return err
	}
	defer fw.Close()

	// Use generic map writer
	pw, err := writer.NewJSONWriter(`{"Tag": "name=name, type=BYTE_ARRAY, encoding=PLAIN, convertedtype=UTF8"}`, fw, 4)
	// pw, err := writer.NewJSONWriter(`{}`, fw, 4)
	if err != nil {
		return err
	}
	defer pw.WriteStop()

	for {
		rec, err := r.Read()
		if err != nil {
			break
		}
		m := make(map[string]interface{})
		for i, v := range rec {
			col := header[i]
			m[col] = v
		}
		if err := pw.Write(m); err != nil {
			return err
		}
	}
	return nil
}
