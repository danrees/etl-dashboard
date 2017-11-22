package storage

import (
	"log"
	"encoding/json"
	"io/ioutil"
	"path"
	"os"
	"strconv"
	"sync"
)


type FileStorage struct {
	storageDirectory string
}

var mutex = &sync.RWMutex{}

func NewFileStorage(storageDirectory string) (FileStorage) {
	//Create storage directory if it doesn't already exist
	if _, err := os.Stat(storageDirectory); os.IsNotExist(err){
		log.Printf("%s does not exist as a directory, it will be created", storageDirectory)
		os.Mkdir(storageDirectory,0755)
	}
	return FileStorage{storageDirectory: storageDirectory}
}

func (fs FileStorage) CreateApplication(app Etl) error {
	mutex.Lock()
	defer mutex.Unlock()

	log.Print("debug ","Creating application: ", app)
	marshaledApp,err := json.Marshal(app)
	if err != nil {
		log.Print("error ", "was unable to marshal app to json ", err)
		//TODO: Consider wrapping and returning your own error
		return err
	}
	err = ioutil.WriteFile(path.Join(fs.storageDirectory,strconv.FormatInt(app.ID,10) + ".json"), marshaledApp,0644)
	if err != nil {
		log.Print("error ","unable to write out to filesystem: ",err)
		return err
	}
	return nil
}

func (fs FileStorage) GetEtlApplication(id int64) (*Etl,error) {
	mutex.RLock()
	defer mutex.RUnlock()
	app, err := ioutil.ReadFile(path.Join(fs.storageDirectory,strconv.FormatInt(id, 10) + ".json"))
	if err != nil {
		log.Print("error ", "unable to find application with ID ", string(id), err)
		return nil,err
	}
	var etlApp Etl
	err = json.Unmarshal(app,&etlApp)
	if err != nil {
		return nil,err
	}
	return &etlApp,nil
}