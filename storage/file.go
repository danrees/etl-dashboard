package storage

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
)

type FileStorage struct {
	storageDirectory string
	autoId           int64
}

var mutex = &sync.RWMutex{}

func NewFileStorage(storageDirectory string) *FileStorage {
	//Create storage directory if it doesn't already exist
	if _, err := os.Stat(storageDirectory); os.IsNotExist(err) {
		log.Printf("%s does not exist as a directory, it will be created", storageDirectory)
		os.Mkdir(storageDirectory, 0755)
	}
	files, err := ioutil.ReadDir(storageDirectory)
	if err != nil {
		panic(err)
	}
	var maxId int64 = 0
	for _, file := range files {
		extension := filepath.Ext(file.Name())
		id, err := strconv.ParseInt(file.Name()[0:len(file.Name())-len(extension)], 10, 64)
		if err != nil {
			continue
		}
		if id > maxId {
			maxId = id
		}
	}
	return &FileStorage{storageDirectory: storageDirectory, autoId: maxId}
}

func (fs *FileStorage) CreateApplication(app Etl) error {
	mutex.Lock()
	defer mutex.Unlock()

	fs.autoId = fs.autoId + 1
	app.ID = fs.autoId
	log.Print("debug ", "Creating application: ", app)
	marshaledApp, err := json.Marshal(app)
	if err != nil {
		log.Print("error ", "was unable to marshal app to json ", err)
		//TODO: Consider wrapping and returning your own error
		return err
	}
	err = ioutil.WriteFile(path.Join(fs.storageDirectory, strconv.FormatInt(app.ID, 10)+".json"), marshaledApp, 0644)
	if err != nil {
		log.Print("error ", "unable to write out to filesystem: ", err)
		return err
	}
	return nil
}

func (fs *FileStorage) GetEtlApplication(id int64) (*Etl, error) {
	mutex.RLock()
	defer mutex.RUnlock()
	app, err := ioutil.ReadFile(path.Join(fs.storageDirectory, strconv.FormatInt(id, 10)+".json"))
	if err != nil {
		log.Print("error ", "unable to find application with ID ", string(id), err)
		return nil, err
	}
	var etlApp Etl
	err = json.Unmarshal(app, &etlApp)
	if err != nil {
		return nil, err
	}
	return &etlApp, nil
}

func (fs *FileStorage) ListEtlApplication() (EtlList, error) {
	mutex.RLock()
	defer mutex.RUnlock()
	fileList, err := ioutil.ReadDir(fs.storageDirectory)
	var etlList = make([]Etl, 0, len(fileList))
	if err != nil {
		return nil, err
	}
	for _, fl := range fileList {
		if !fl.IsDir() {
			var foundEtl Etl
			b, err := ioutil.ReadFile(path.Join(fs.storageDirectory, fl.Name()))
			if err != nil {
				return nil, err
			}
			err = json.Unmarshal(b, &foundEtl)
			if err != nil {
				return nil, err
			}
			etlList = append(etlList, foundEtl)
		}
	}
	return etlList, nil
}

func (fs *FileStorage) DeleteEtlApplication(id int64) error {
	return errors.New("Unimplemented method")
}
