package main

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/nnurry/pds/cmd/metadata/app"
	metadata "github.com/nnurry/pds/cmd/metadata/metadata"
)

func main() {
	app.Setup()
	r := chi.NewRouter()
	r.Use(middleware.Logger)

	filterController := metadata.NewFilterController()
	cardinalController := metadata.NewCardinalController()
	r.Use(middleware.Compress(5, "application/json"))

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hi!"))
	})

	r.Route("/filter", func(r chi.Router) {
		r.Get("/available", filterController.Available)
		r.Post("/create", filterController.Create)
		r.Get("/list", filterController.List)
		r.Post("/exists", filterController.Exists)
		r.Post("/add", filterController.Add)
	})

	r.Route("/cardinal", func(r chi.Router) {
		r.Get("/available", cardinalController.Available)
		r.Get("/list", cardinalController.List)
		r.Get("/cardinality", cardinalController.Cardinality)
		r.Post("/add", cardinalController.Add)
	})

	http.ListenAndServe(":3000", r)
}
