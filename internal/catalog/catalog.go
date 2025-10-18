package catalog

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Catalog struct {
	db *sql.DB
}

type Dataset struct {
	ID         int64
	Name       string
	Type       string
	StorageURI string
	SizeBytes  int64
	Cols       string // simple CSV of columns or JSON
	CreatedAt  time.Time
}

func New(path string) (*Catalog, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	c := &Catalog{db: db}
	if err := c.migrate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Catalog) migrate() error {
	s := `
	CREATE TABLE IF NOT EXISTS datasets (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT,
		type TEXT,
		storage_uri TEXT,
		size_bytes INTEGER,
		cols TEXT,
		created_at DATETIME
	);`
	_, err := c.db.Exec(s)
	return err
}

func (c *Catalog) AddDataset(name, dtype, uri string, size int64, cols string) (int64, error) {
	now := time.Now().UTC()
	fmt.Printf("Adding dataset: name=%s, type=%s, uri=%s, size=%d, cols=%s, created_at=%s\n",
		name, dtype, uri, size, cols, now.Format(time.RFC3339Nano))
	res, err := c.db.Exec(`INSERT INTO datasets(name,type,storage_uri,size_bytes,cols,created_at) VALUES(?,?,?,?,?,?)`,
		name, dtype, uri, size, cols, now)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (c *Catalog) List() ([]Dataset, error) {
	rows, err := c.db.Query(`SELECT id,name,type,storage_uri,size_bytes,cols,created_at FROM datasets ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Dataset
	for rows.Next() {
		var d Dataset
		var ts string
		if err := rows.Scan(&d.ID, &d.Name, &d.Type, &d.StorageURI, &d.SizeBytes, &d.Cols, &ts); err != nil {
			return nil, err
		}
		d.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, d)
	}
	return out, nil
}

func (c *Catalog) Close() error {
	return c.db.Close()
}
