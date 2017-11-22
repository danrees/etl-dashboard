package storage

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"strconv"
)

type Etl struct {
	ID          int64             `json:"id"`
	Name        string            `json:"name"`
	StartKey    string            `json:"startKey"`
	CompleteKey string            `json:"completeKey"`
	Parameters  map[string]string `json:"parameters"`
}



type EtlHandler struct {
	storageEngine Storage
}

func New(storage Storage) EtlHandler{
	return EtlHandler{storage}
}

func (etl *EtlHandler)GetCreateEtlHandler() (func(w http.ResponseWriter, r *http.Request)){
	return func(w http.ResponseWriter, r *http.Request) {
		var applicationEtl Etl
		err := json.NewDecoder(r.Body).Decode(&applicationEtl)
		if err != nil {
			log.Print("error ", "Failed to decode json payload ", err)
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		err = etl.storageEngine.CreateApplication(applicationEtl)
		if err != nil {
			log.Print("error ", "Failed to create a new application ", err.Error())
			http.Error(w,err.Error(),http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(http.StatusCreated)
		return
	}
}

func (etl *EtlHandler)GetEtlHandler() (func(w http.ResponseWriter, r *http.Request)){
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		idParam, ok := vars["id"]
		if !ok {
			http.Error(w, "No ID sent with request", http.StatusInternalServerError)
			return
		}
		id, err := strconv.ParseInt(idParam, 10, 64)
		if err != nil {
			http.Error(w,err.Error(),http.StatusInternalServerError)
			return
		}
		app, err := etl.storageEngine.GetEtlApplication(id)
		if err != nil {
			http.Error(w,err.Error(),http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		if err = json.NewEncoder(w).Encode(app); err != nil {
			http.Error(w,err.Error(),http.StatusInternalServerError)
			return
		}
		return
	}
}

