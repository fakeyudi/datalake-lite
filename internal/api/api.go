package api

import (
	"fmt"
	"io"
	"net/http"

	"github.com/fakeyudi/datalake-lite/internal/catalog"
	"github.com/fakeyudi/datalake-lite/internal/storage"
)

type Server struct {
	Catalog *catalog.Catalog
	Storage storage.Storage
	Mux     *http.ServeMux
}

func New(c *catalog.Catalog, s storage.Storage) *Server {
	srv := &Server{Catalog: c, Storage: s, Mux: http.NewServeMux()}
	srv.routes()
	return srv
}

func (s *Server) routes() {
	s.Mux.HandleFunc("/datasets", s.handleList)
	s.Mux.HandleFunc("/download", s.handleDownload) // ?uri=...
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	ds, err := s.Catalog.List()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	for _, d := range ds {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\n", d.ID, d.Name, d.Type, d.StorageURI)
	}
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	if uri == "" {
		http.Error(w, "missing uri", 400)
		return
	}
	rc, err := s.Storage.Get(uri)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rc.Close()
	io.Copy(w, rc)
}
