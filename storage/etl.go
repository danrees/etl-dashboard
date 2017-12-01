package storage

import (
	"encoding/json"
	"etl-dashboard/messaging"
	"github.com/gorilla/mux"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"
	"html/template"
)

type Etl struct {
	ID          int64    `json:"id"`
	Name        string   `json:"name"`
	StartKey    string   `json:"startKey"`
	CompleteKey string   `json:"completeKey"`
	Parameters  []string `json:"parameters"`
}

type EtlList []Etl

type EtlHandler struct {
	storageEngine Storage
	sender        messaging.Sender
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randomString(l int) string {
	bytes := make([]byte, l)

	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}

	return string(bytes)
}

func New(storage Storage, sender messaging.Sender) EtlHandler {
	return EtlHandler{storage, sender}
}

func (etl *EtlHandler) GetCreateEtlHandler() func(w http.ResponseWriter, r *http.Request) {
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
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(http.StatusCreated)
		return
	}
}

func (etl *EtlHandler) GetListEtlHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		etlList, err := etl.storageEngine.ListEtlApplication()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		if err = json.NewEncoder(w).Encode(etlList); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

	}
}

func (etl *EtlHandler) GetEtlHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		idParam, ok := vars["id"]
		if !ok {
			http.Error(w, "No ID sent with request", http.StatusInternalServerError)
			return
		}
		id, err := strconv.ParseInt(idParam, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		app, err := etl.storageEngine.GetEtlApplication(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		if err = json.NewEncoder(w).Encode(app); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		return
	}
}

func (etl *EtlHandler) GetStartEtlHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		idParam, ok := vars["id"]
		if !ok {
			http.Error(w, "Unable to parse id from url", http.StatusInternalServerError)
			return
		}
		id, err := strconv.ParseInt(idParam, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		app, err := etl.storageEngine.GetEtlApplication(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		msg := messaging.Message{Env: make(map[string]string)}
		r.ParseForm()
		//Grab the urlencoded values from the post, for each parameter in the etl app object
		//put the found value in the message
		for _, p := range app.Parameters {
			param := r.Form.Get(p)
			msg.Env[p] = param
		}
		etl.sender.Send(msg, app.StartKey, randomString(32))
	}
}

func (etl *EtlHandler) GetStartEtlPageHandler() func(w http.ResponseWriter, r *http.Request){
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		idParam, ok := vars["id"]
		if !ok {
			http.Error(w,"Invalid parameter provided", http.StatusInternalServerError)
		}
		id, err := strconv.ParseInt(idParam,10, 64)
		if err != nil {
			http.Error(w,err.Error(),http.StatusInternalServerError)
		}

		t,err := template.ParseFiles("templates/run-app.html")
		if err != nil {
			http.Error(w,err.Error(), http.StatusInternalServerError)
		}
		app, err := etl.storageEngine.GetEtlApplication(id)
		if err != nil {
			http.Error(w,err.Error(), http.StatusInternalServerError)
		}
		t.Execute(w,app)
	}
}

func (etl *EtlHandler) GetListEtlPageHandler() func(w http.ResponseWriter, r *http.Request){
	return func(w http.ResponseWriter, r *http.Request) {

		t,err := template.ParseFiles("templates/list-etls.html")
		if err != nil {
			http.Error(w,err.Error(), http.StatusInternalServerError)
		}
		apps, err := etl.storageEngine.ListEtlApplication()
		if err != nil {
			http.Error(w,err.Error(), http.StatusInternalServerError)
		}
		t.Execute(w,apps)
	}
}
