# Datalake-Lite

Lightweight local data lake for Go projects — handles ingestion, storage, and simple cataloging of files.

## 🚀 Quickstart

```bash
# build CLI
go build -o bin/dl ./cmd/dl

# build listener
go build -o bin/listener ./cmd/listener
```

Run CLI manually:
```bash
./bin/dl -op ingest -file ./data/sample.csv
./bin/dl -op list

or

go run cmd/dl/main.go -op ingest -file ./data/sample.csv
go run cmd/dl/main.go -op list
```

Run the listener (auto-ingests files added to a directory):
```bash
./bin/listener -watch ./data/incoming -storage ./data -db dl_catalog.db

or

go run cmd/listener/main.go -watch ./data/incoming -storage ./data -db dl_catalog.db
```

Drop files into `./data/incoming` — they’ll be automatically detected and ingested.

## 🐳 Docker Usage

```bash
docker build -t datalake-lite:latest .
docker run --rm -v $(pwd)/data:/app/data -v $(pwd)/dl_catalog.db:/app/dl_catalog.db datalake-lite:latest /app/bin/listener -watch /app/data/incoming -storage /app/data -db /app/dl_catalog.db
```

## 🧩 Structure

```
cmd/
 ├── dl/          # Main CLI
 └── listener/    # Directory watcher
internal/
 ├── listener/    # Watches directories
 ├── queue/       # Queue abstractions (in-memory / RabbitMQ stub)
 ├── ingest/      # File ingestion logic
 ├── catalog/     # SQLite catalog
 ├── storage/     # Local storage handling
 └── api/         # Lightweight API for preview and download
```

## 🧠 Future Enhancements

- RabbitMQ or Redis-based queue backend
- Automatic Parquet conversion during ingestion
- Cloud sync adapters (S3 / GCS)
- Metrics and retry handling
