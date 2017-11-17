package main

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

func CreateEtlApplication(app Etl) error {
	log.Printf("Would create application: %v", app)
	return nil
}

func GetEtlApplication(id int64) (Etl, error) {
	log.Printf("Recieved %d", id)
	return Etl{ID: id}, nil
}

func CreateEtlHandler(w http.ResponseWriter, r *http.Request) {
	var applicationEtl Etl
	err := json.NewDecoder(r.Body).Decode(&applicationEtl)
	if err != nil {
		log.Print("error ", "Failed to decode json payload ", err)
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(http.StatusUnprocessableEntity)
		//TODO: Fix the error handling here
		if err := json.NewEncoder(w).Encode(map[string]string{"msg": err.Error()}); err != nil {
			panic(err)
		}
		return
	}
	err = CreateEtlApplication(applicationEtl)
	if err != nil {
		log.Print("error ", "Failed to create a new application ", err.Error())
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(http.StatusInternalServerError)
		if err := json.NewEncoder(w).Encode(map[string]error{"msg": err}); err != nil {
			panic(err)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	w.WriteHeader(http.StatusCreated)
	return
}

func GetEtlHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	idParam, ok := vars["id"]
	if !ok {
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(http.StatusInternalServerError)
		//TODO: Figure out something meaningful to return here
		return
	}
	id, err := strconv.ParseInt(idParam, 10, 64)
	if err != nil {
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(http.StatusInternalServerError)
		if err := json.NewEncoder(w).Encode(err); err != nil {
			panic(err)
		}
		return
	}
	app, err := GetEtlApplication(id)
	if err != nil {
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(http.StatusInternalServerError)
		if err := json.NewEncoder(w).Encode(err); err != nil {
			panic(err)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err = json.NewEncoder(w).Encode(app); err != nil {
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(http.StatusInternalServerError)
		if err := json.NewEncoder(w).Encode(err); err != nil {
			panic(err)
		}
		return
	}
	return
}
