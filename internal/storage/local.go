package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type LocalStorage struct {
	Base string
}

func NewLocal(base string) *LocalStorage {
	os.MkdirAll(base, 0o755)
	return &LocalStorage{Base: base}
}

func (l *LocalStorage) Put(path string, r io.Reader) (string, int64, error) {
	full := filepath.Join(l.Base, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return "", 0, err
	}
	f, err := os.Create(full)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	n, err := io.Copy(f, r)
	if err != nil {
		return "", 0, err
	}
	uri := "file://" + full
	return uri, n, nil
}

func (l *LocalStorage) Get(uri string) (io.ReadCloser, error) {
	// expect uri like file:///abs/path
	const prefix = "file://"
	if len(uri) > len(prefix) && uri[:len(prefix)] == prefix {
		path := uri[len(prefix):]
		return os.Open(path)
	}
	return nil, fmt.Errorf("unsupported uri: %s", uri)
}

func (l *LocalStorage) Exists(uri string) (bool, error) {
	const prefix = "file://"
	if len(uri) > len(prefix) && uri[:len(prefix)] == prefix {
		_, err := os.Stat(uri[len(prefix):])
		if err == nil {
			return true, nil
		}
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return false, nil
}
