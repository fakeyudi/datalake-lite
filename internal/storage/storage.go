package storage

import "io"

type Storage interface {
	Put(path string, r io.Reader) (uri string, size int64, err error)
	Get(uri string) (rc io.ReadCloser, err error)
	Exists(uri string) (bool, error)
}
