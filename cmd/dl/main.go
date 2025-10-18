package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/fakeyudi/datalake-lite/internal/catalog"
	"github.com/fakeyudi/datalake-lite/internal/ingest"
	"github.com/fakeyudi/datalake-lite/internal/storage"
)

func main() {
	dbPath := flag.String("db", "dl_catalog.db", "path to sqlite catalog")
	storageDir := flag.String("storage", "./data", "local storage path")
	op := flag.String("op", "help", "operation: ingest | list")
	file := flag.String("file", "", "local file to ingest (for ingest op)")
	name := flag.String("name", "", "dataset name (optional)")
	outPath := flag.String("out", "", "optional output path (for CSV→Parquet conversion)")
	flag.Parse()

	if *op == "help" {
		usage()
		return
	}

	cat, err := catalog.New(*dbPath)
	if err != nil {
		log.Fatalf("catalog init: %v", err)
	}
	defer cat.Close()

	st := storage.NewLocal(*storageDir)
	ing := ingest.New(st, cat)

	switch *op {
	case "ingest":
		if *file == "" {
			log.Fatalf("must provide -file for ingest")
		}

		// If -out is provided, convert CSV→Parquet instead of normal ingestion
		if *outPath != "" {
			err := ingest.ConvertCSVToParquet(*file, *outPath)
			if err != nil {
				log.Fatalf("CSV→Parquet conversion failed: %v", err)
			}
			fmt.Printf("Converted %s to Parquet at %s\n", *file, *outPath)
			return
		}

		// regular ingestion
		id, uri, err := ing.IngestFile(*file, *name)
		if err != nil {
			log.Fatalf("ingest failed: %v", err)
		}
		fmt.Printf("Ingested dataset id=%d uri=%s\n", id, uri)
	case "list":
		list(cat)
	default:
		usage()
	}
}

func usage() {
	fmt.Println(`datalake-lite CLI
Usage:
  -op ingest -file /path/to/file [-name datasetName]
  -op list
Flags:
`)
	os.Exit(1)
}

func list(cat *catalog.Catalog) {
	ds, err := cat.List()
	if err != nil {
		log.Fatalf("list failed: %v", err)
	}
	for _, d := range ds {
		fmt.Printf("[%d] %s (%s) size=%d uri=%s cols=%s created=%v\n", d.ID, d.Name, d.Type, d.SizeBytes, d.StorageURI, d.Cols, d.CreatedAt)
	}
}
