package ingest

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/fakeyudi/datalake-lite/internal/catalog"
	"github.com/fakeyudi/datalake-lite/internal/storage"
)

type Ingester struct {
	Storage storage.Storage
	Catalog *catalog.Catalog
}

func New(s storage.Storage, c *catalog.Catalog) *Ingester {
	return &Ingester{Storage: s, Catalog: c}
}

// simple function: ingest local file path, give dataset name (optional)
func (in *Ingester) IngestFile(localPath, datasetName string) (int64, string, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()

	// default dataset name if empty:
	if datasetName == "" {
		datasetName = filepath.Base(localPath)
	}
	// storage path: datasets/{name}/{timestamp}_{basename}
	storagePath := filepath.Join("datasets", datasetName, filepath.Base(localPath))

	uri, size, err := in.Storage.Put(storagePath, f)
	if err != nil {
		return 0, "", err
	}

	// Try to detect CSV columns if .csv
	cols := ""
	if strings.HasSuffix(strings.ToLower(localPath), ".csv") {
		if c, e := previewCSVColumns(localPath); e == nil {
			cols = strings.Join(c, ",")
		}
	}

	id, err := in.Catalog.AddDataset(datasetName, detectType(localPath), uri, size, cols)
	return id, uri, err
}

func detectType(path string) string {
	path = strings.ToLower(path)
	switch {
	case strings.HasSuffix(path, ".csv"):
		return "csv"
	case strings.HasSuffix(path, ".json"):
		return "json"
	case strings.HasSuffix(path, ".parquet"):
		return "parquet"
	default:
		return "blob"
	}
}

// simplistic CSV preview: reads first non-empty row as header
func previewCSVColumns(localPath string) ([]string, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReader(f))
	for {
		record, err := r.Read()
		if err == io.EOF {
			return nil, fmt.Errorf("no rows in csv")
		}
		if err != nil {
			return nil, err
		}
		// skip empty rows
		nonEmpty := false
		for _, v := range record {
			if strings.TrimSpace(v) != "" {
				nonEmpty = true
				break
			}
		}
		if nonEmpty {
			return record, nil
		}
	}
}
