package metadata

import (
	"encoding/json"
	"log"
	"net/http"
)

type filterController struct {
	service *filterService
}

func NewFilterController() *filterController {
	return &filterController{
		service: NewFilterService(),
	}
}

func (c *filterController) Available(w http.ResponseWriter, r *http.Request) {

}

func (c *filterController) List(w http.ResponseWriter, r *http.Request) {

}

func (c *filterController) Create(w http.ResponseWriter, r *http.Request) {
	blob, err := ReadBodyToBlob(r.Body)
	if err != nil {
		http.Error(w, "can't decode json: "+err.Error(), http.StatusBadRequest)
	}

	payload := struct {
		Type           string  `json:"type"`
		Key            string  `json:"key"`
		MaxCardinality uint    `json:"max_cardinality"`
		ErrorRate      float64 `json:"error_rate"`
	}{}

	err = LoadBlobToJson(blob, &payload)

	if err != nil {
		http.Error(w, "can't decode json: "+err.Error(), http.StatusBadRequest)
		return
	}

	filter, err := c.service.CreateFilter(
		payload.Type, payload.Key,
		payload.MaxCardinality, payload.ErrorRate,
	)

	if err != nil {
		http.Error(w, "can't create filter: "+err.Error(), http.StatusBadRequest)
		return
	}

	// let's decide what to do with the filter later
	log.Println("created filter:", filter)

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

func (c *filterController) Exists(w http.ResponseWriter, r *http.Request) {

}

func (c *filterController) Add(w http.ResponseWriter, r *http.Request) {

}

////////////////////////////////////////////////////////////////

type cardinalController struct {
	service *cardinalService
}

func NewCardinalController() *cardinalController {
	return &cardinalController{
		service: NewCardinalService(),
	}
}

func (c *cardinalController) Available(w http.ResponseWriter, r *http.Request) {

}

func (c *cardinalController) List(w http.ResponseWriter, r *http.Request) {

}

func (c *cardinalController) Create(w http.ResponseWriter, r *http.Request) {
	blob, err := ReadBodyToBlob(r.Body)
	if err != nil {
		http.Error(w, "can't decode json: "+err.Error(), http.StatusBadRequest)
	}

	payload := struct {
		Type string `json:"type"`
		Key  string `json:"key"`
	}{}

	err = LoadBlobToJson(blob, &payload)

	if err != nil {
		http.Error(w, "can't decode json: "+err.Error(), http.StatusBadRequest)
		return
	}

	cardinal, err := c.service.CreateCardinal(payload.Type, payload.Key)

	if err != nil {
		http.Error(w, "can't create cardinal: "+err.Error(), http.StatusBadRequest)
		return
	}

	// let's decide what to do with the cardinal later
	log.Println("created cardinal:", cardinal)

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

func (c *cardinalController) Cardinality(w http.ResponseWriter, r *http.Request) {
	blob, err := ReadBodyToBlob(r.Body)
	if err != nil {
		http.Error(w, "can't decode json: "+err.Error(), http.StatusBadRequest)
	}

	payload := struct {
		Type string `json:"type"`
		Key  string `json:"key"`
	}{}

	err = LoadBlobToJson(blob, &payload)

	if err != nil {
		http.Error(w, "can't decode json: "+err.Error(), http.StatusBadRequest)
		return
	}

}

func (c *cardinalController) Add(w http.ResponseWriter, r *http.Request) {

}
