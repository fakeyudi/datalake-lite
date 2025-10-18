package main

import (
	"log"
	"net/http"

	"github.com/fakeyudi/datalake-lite/internal/api"
	"github.com/fakeyudi/datalake-lite/internal/catalog"
	"github.com/fakeyudi/datalake-lite/internal/storage"
)

func main() {
	cat, err := catalog.New("dl_catalog.db")
	if err != nil { log.Fatal(err) }
	st := storage.NewLocal("./data")
	s := api.New(cat, st)
	log.Println("listening :8080")
	log.Fatal(http.ListenAndServe(":8080", s.Mux))
}
