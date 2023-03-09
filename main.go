package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

func main() {
	var broker = "mqtt"
	//var broker = "localhost"
	var port = 1883
	fmt.Println("starting application")

	//options.SetClientID("test_client")
	// no username or password is use, as allow allow_anonymous is set to true
	mqttService, err := NewMQTTClient(port, broker, "", "")
	if err != nil {
		panic(err)
	}

	topic := "sensor_data/temperature"
	type publishMessage struct {
		Message string `json:"message"`
	}

	wg := sync.WaitGroup{}

	num := 10
	wg.Add(num)

	go func() {
		for i := 0; i < num; i++ {
			fmt.Println("publishing to topic")
			message, _ := json.Marshal(&publishMessage{Message: fmt.Sprintf("message %d", i)})
			err := mqttService.Publish(topic, message)
			if err != nil {
				fmt.Println(err)
			}
			time.Sleep(time.Second)
		}
	}()

	respDataChan := make(chan []byte)
	quit := make(chan bool, 1)

	go func() {
		wg.Wait()
		quit <- true
		close(respDataChan)
	}()

	go func() {
		mqttService.Subscribe(topic, respDataChan)
	}()

	for {
		select {
		case respData := <-respDataChan:
			wg.Done()
			fmt.Printf("got response %v \n", string(respData))
		case <-quit:
			fmt.Println("program existed")
			return
		}
	}
}
