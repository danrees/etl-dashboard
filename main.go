package main

import (
	"etl-dashboard/messaging"
	"etl-dashboard/storage"
	"etl-dashboard/websocket"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/streadway/amqp"
	"log"
	"net/http"
	"os/user"
	"path"
)

func main() {
	usr, err := user.Current()
	if err != nil {
		log.Fatal("fatal ", "Unable to retrieve user information ", err)
	}
	var rabbitUser = flag.String("user", "guest", "RabbitMQ user name")
	var rabbitPassword = flag.String("password", "guest", "RabbitMQ password")
	var rabbitHost = flag.String("host", "localhost", "RabbitMQ host")
	var rabbitPort = flag.String("port", "5672", "RabbitMQ port")
	//var sendKey = flag.String("routingKey", "", "Routing that is sent on")
	var dataDir = flag.String("dataDir", path.Join(usr.HomeDir, ".etldashboard", "data"), "Directory to save data files")

	flag.Parse()

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s", *rabbitUser, *rabbitPassword, *rabbitHost, *rabbitPort))

	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to retrieve connection to rabbitmq server at %v:%v using provided credentials", rabbitHost, rabbitPort))
		panic(err)

	}
	defer conn.Close()
	exchangeName := "etl_exchange"
	//routingKey := "test.key"

	publishChannel, err := conn.Channel()
	if err != nil {
		log.Fatal("Unable to create publish channel for rabbitmq", err)
		panic(err)
	}
	defer publishChannel.Close()
	var publisher messaging.Sender = messaging.NewRabbitMessenger(publishChannel, exchangeName)

	subscribeChannel, err := conn.Channel()
	if err != nil {
		log.Fatal("Unable to create subscribe channel for rabbitmq", err)
		panic(err)
	}
	defer subscribeChannel.Close()
	var subscriber messaging.Watcher = messaging.NewRabbitMessenger(subscribeChannel, exchangeName)

	broadcast := make(chan string)
	//Start up the watcher ... I hope
	go subscriber.Watch("#",&broadcast)

	log.Println("[*] Watcher starting, waiting for messages")

	etlHandler := storage.New(storage.NewFileStorage(*dataDir), publisher)


	go websocket.HandleMessages(broadcast)

	r := mux.NewRouter()

	r.HandleFunc("/ws", websocket.GetWebsocketHandler(broadcast))

	/*r.
	Methods("POST").
	Path("/message").
	HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var msg messaging.Message

		err := json.NewDecoder(request.Body).Decode(&msg)
		if err != nil {
			http.Error(writer, err.Error(), 400)
			return
		}
		err = publisher.Send(msg, *sendKey, randomString(32))
	})*/
	r.Path("/etl").Methods("GET").HandlerFunc(etlHandler.GetListEtlPageHandler())
	r.Path("/etl/{id}/start").Methods("GET").HandlerFunc(etlHandler.GetStartEtlPageHandler())
	r.Path("/api/etl").Methods("POST").HandlerFunc(etlHandler.GetCreateEtlHandler())
	r.Path("/api/etl").Methods("GET").HandlerFunc(etlHandler.GetListEtlHandler())
	r.Methods("GET").Path("/api/etl/{id}").HandlerFunc(etlHandler.GetEtlHandler())
	r.Methods("POST").Path("/api/etl/{id}/start").HandlerFunc(etlHandler.GetStartEtlHandler())

	r.PathPrefix("/").Handler(http.StripPrefix("/", http.FileServer(http.Dir("public/"))))
	http.ListenAndServe(":8002", r)
}
