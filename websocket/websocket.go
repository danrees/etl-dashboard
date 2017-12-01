package websocket

import (
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"sync"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var connections = make(map[*websocket.Conn]bool)
var mutex = sync.Mutex{}

type TestMessage struct {
	Message string `json:"msg"`
}

func GetWebsocketHandler(broadcast chan string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer conn.Close()
		mutex.Lock()
		connections[conn] = true
		mutex.Unlock()
		for {
			var msg string
			err = conn.ReadJSON(&msg)
			if err != nil {
				log.Print("ERROR ", err)
				break
			}
			broadcast <- msg
		}
	}
}

func HandleMessages(broadcast chan string) {
	for {
		msg := <-broadcast
		for client := range connections {

			err := client.WriteJSON(msg)
			if err != nil {
				log.Print("ERROR ", err)
				client.Close()
			}
		}
	}
}
