package main

import (
    "log"
    "gopkg.in/redis.v5"
	"encoding/json"
	"net/http"
)

const queueName = "messages"

type Message struct{
	Id int
	Message string
}

func send_raw(payload *Message, client *redis.Client) {
	data, err := json.Marshal(payload)

	ok, err := client.LPush(queueName, data).Result()

	if err != nil {
		log.Printf("Error: %#v", err)
	} else {
		log.Printf("OK: %#v", ok)
	}
}

func send(id int, message string, client *redis.Client) {
	payload := Message {
		Id : id,
		Message: message,
	}

    send_raw(&payload, client)
}

func receive(client *redis.Client) {
	log.Printf("Consumer started.")
	for {
		data, _ := client.BRPop(0, queueName).Result()
		log.Printf("Consumer received: %#v", data)
	}
}

type MyHandler struct {}

func (h *MyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    payload := &Message{}
    decoder := json.NewDecoder(r.Body)
    err := decoder.Decode(payload)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    client := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "", // no password set
        DB:       0,  // use default DB
    })
	defer client.Close()

   send_raw(payload, client)
}

func main() {
    client := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "", // no password set
        DB:       0,  // use default DB
    })

	go receive(client)

	send(1, "test", client)

    addr := "127.0.0.1:8081"
    handler := &MyHandler{}
    log.Printf("Running web server on: http://%s\n", addr)
    log.Fatal(http.ListenAndServe(addr, handler))
}
