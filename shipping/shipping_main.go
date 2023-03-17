package main

import (
	"fmt"
	"math/rand"
	"time"

	"./brokers"
)

var availableCustomers = map[string][]string{
	"Company":    {"AMAZON", "GOOGLE", "META"},
	"GOVERNMENT": {"DEFENSE_DEPARTMENT", "FOREIGN_OF_MINISTRY"},
	"INDIVIDUAL": {"NAME1", "NAME2", "NAME3", "NAME4"},
}

var availableVendors = map[string][]string{
	"CAR":        {"BMW", "TOYOTA", "HONDA", "SUBARU"},
	"APPLIANCES": {"LG", "SONY", "PANASONIC", "DYSON"},
	"3C":         {"APPLE", "HP", "BESTBUY"},
}

func vendorOrderPublisher(broker *brokers.Broker) {
	topicVendors := make([]string, 0, len(availableVendors))
	for k, v := range availableVendors {
		for _, name := range v {
			topicVendors = append(topicVendors, "VENDOR/"+k+"/"+name)
		}
	}

	for {
		randVendor := topicVendors[rand.Intn(len(topicVendors))] // all topic values.
		msg := fmt.Sprintf("%f", rand.Float64())
		attributes := map[string]string{
			"items":    "item name",
			"price":    "xxx",
			"quantity": "zzz",
		}
		// fmt.Printf("Publishing %s to %s topic\n", msg, randKey)
		go broker.Publish(randVendor, msg, attributes)
		// Uncomment if you want to broadcast to all topics.
		// go broker.Broadcast(msg, topicValues, attributes)
		r := rand.Intn(4)
		time.Sleep(time.Duration(r) * time.Second) //sleep for random secs.
	}
}

func customerOrderPublisher(broker *brokers.Broker) {
	topicCustomers := make([]string, 0, len(availableCustomers))
	for k, v := range availableCustomers {
		for _, name := range v {
			topicCustomers = append(topicCustomers, "CUSTOMER/"+k+"/"+name)
		}
	}

	for {
		randCustomer := topicCustomers[rand.Intn(len(topicCustomers))] // all topic values.
		msg := fmt.Sprintf("%f", rand.Float64())
		attributes := map[string]string{
			"items":    "item name",
			"price":    "xxx",
			"quantity": "zzz",
		}
		// fmt.Printf("Publishing %s to %s topic\n", msg, randKey)
		go broker.Publish(randCustomer, msg, attributes)
		// Uncomment if you want to broadcast to all topics.
		// go broker.Broadcast(msg, topicValues, attributes)
		r := rand.Intn(4)
		time.Sleep(time.Duration(r) * time.Second) //sleep for random secs.
	}
}

func main() {
	////////////////////
	// Initialization //
	////////////////////
	fmt.Printf("******Initialization******\n")
	// Construct Mosquitto and connection with client-side server
	var mosquitto = "mqtt"
	var port = 1883
	mqttService, err := brokers.NewMQTTClient(port, mosquitto, "", "")
	if err != nil {
		panic(err)
	}

	// construct a master broker
	Mbroker := brokers.NewMasterBroker(10)
	// construct new edge brokers.
	Ebroker1 := brokers.NewBroker(10)
	Ebroker2 := brokers.NewBroker(10)
	Ebroker3 := brokers.NewBroker(10)
	// create new subscriber(local client)
	s11 := Ebroker1.AddSubscriber(10)
	s12 := Ebroker1.AddSubscriber(10)
	s21 := Ebroker2.AddSubscriber(10)
	s22 := Ebroker2.AddSubscriber(10)
	s31 := Ebroker2.AddSubscriber(10)
	s32 := Ebroker2.AddSubscriber(10)
	s33 := Ebroker2.AddSubscriber(10)

	//time.Sleep(5 * time.Second)
	fmt.Printf("\n\n")

	// subscribe: edge broker -> master broker
	/* 1. all edge brokers can subscribe to master broker
	 * 2. Subscribed topic format: starts from the name of the other client-side servers to receive and then process where the messages come from and the following content refer to the details of other client-side info
	 */
	//	Mbroker.Subscribe(Ebroker1, "SHIPPING/3C/APPLE/STATUS")
	//	Mbroker.Subscribe(Ebroker2, "NOTIFICATION/CUSTOMER/CAR/TOYOTA/STATUS")
	//	Mbroker.Subscribe(Ebroker3, "NOTIFICATION/VENDOR/CAR/HONDA/STATUS")
	//
	//	//Mbroker.Publish("SHIPPING/3C/APPLE/STATUS", "EB -> MB, then send to MQTT", attributeAA)
	//	// subscribe: local client -> edge broker //
	//	/* 1. one local client only connect to one edge broker, but can subscribe multiple topics
	//	 * 2. Subscribed topic format: starts from the name of the current client-side server so as to identify where
	//	 */
	//	Ebroker1.Subscribe(s1, "PACKAGE/VENDOR/CAR/HONDA/STATUS")
	//	Ebroker1.Subscribe(s1, "NOTIFICATION/VENDOR/3C/APPLE/STATUS")
	//	Ebroker2.Subscribe(s2, "NOTIFICATION/CUSTOMER/CAR/TOYOTA/STATUS")
	//	Ebroker2.Subscribe(s2, "NOTIFICATION/VENDOR/APPLIANCES/LG/STATUS")
	//
	//	Ebroker1.Subscribe(s1, "VENDOR/CAR/HONDA")
	//
	//	go (func() {
	//		// sleep for 5 sec, and then subscribe for topic DOT for s2
	//		time.Sleep(3 * time.Second)
	//		Ebroker2.Subscribe(s2, "VENDOR/CAR/TOYOTA")
	//		Ebroker2.Subscribe(Mbroker, "PACKAGE/APPLIANCES/SONY")
	//
	//		//fmt.Println(Mbroker.GetEdgeTopics(Ebroker1.id))
	//	})()
	//
	//	go (func() {
	//		// sleep for 5 sec, and then unsubscribe for topic SOL for s2
	//		time.Sleep(5 * time.Second)
	//
	//		if Ebroker2.Unsubscribe(s2, "VENDOR/CAR/TOYOTA") {
	//			fmt.Printf("Total subscribers for topic TOYOTA is %v\n", Ebroker2.GetSubscribers("TOYOTA"))
	//		}
	//		if Ebroker2.Unsubscribe(s2, "VENDOR/CAR/BMW") {
	//			fmt.Printf("Total subscribers for topic BMW is %v\n", Ebroker2.GetSubscribers("BMW"))
	//		}
	//		if Ebroker1.Unsubscribe(s1, "VENDOR/CAR/BMW") {
	//			fmt.Printf("Total subscribers for topic BMW is %v\n", Ebroker1.GetSubscribers("BMW"))
	//		}
	//		if Ebroker2.Unsubscribe(Mbroker, "PACKAGE/APPLIANCES/SONY") {
	//			fmt.Printf("Total subscribers for topic PACKAGE/APPLIANCES/SONY is %v\n", Ebroker2.GetSubscribers("PACKAGE/APPLIANCES/SONY"))
	//		}
	//	})()
	//
	//	go (func() {
	//		// sleep for 5 sec, and then unsubscribe for topic SOL for s2
	//		time.Sleep(10 * time.Second)
	//		Ebroker2.RemoveSubscriber(s2)
	//		fmt.Printf("RemoveSubscriber: Total subscribers for topic APPLE is %v\n", Ebroker2.GetSubscribers("VENDOR/3C/APPLE"))
	//	})()
	//
	//	//// Concurrently publish the values.
	//	go vendorOrderPublisher(Ebroker1)
	//	go customerOrderPublisher(Ebroker2)

	//// Concurrently publish the messages to Mosquitto
	//go (func() {
	//	message, _ := json.Marshal("String to be encoded")
	//	err := mqttService.Publish("SHIPPING/3C/APPLE/STATUS", message)
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//	time.Sleep(time.Second)
	//})()

	//// Scenario 2: user to packaging: ordered -> package: confirmed ->
	//shipping: deliver -> notification: user
	// Listen for Master broker to send message to MQTT and Edge broker
	respDataChan := make(chan string)
	go Mbroker.Publish2MQTT(mqttService)
	go Mbroker.Publish2EdgeBroker(respDataChan)

	// MB sub MQTT
	go (func() {
		topic := "PACKAGE/SHIPPING/ITEM1/REQUEST"
		mqttService.Subscribe(topic, respDataChan)
	})()
	// MB sub EB
	Ebroker1.Subscribe(Mbroker, "NOTIFICATION/ITEM1/RESPONSE")
	// EB sub MB
	Mbroker.Subscribe(Ebroker1, "PACKAGE/ITEM1/REQUEST")
	// client sub EB
	Ebroker1.Subscribe(s11, "PACKAGE/ITEM1/REQUEST")
	//// publish to s1

	go (func() {
		attributes := map[string]string{
			"items":     "ITEM1",
			"price":     "500$",
			"quantity":  "10",
			"completed": "FAIL",
		}
		for i := 0; i < 5; i++ {
			message := "Item1 package failed. Send fail message from PACKAGE server to NOTIFICATION server"
			Ebroker1.Publish("NOTIFICATION/ITEM1/RESPONSE", message, attributes)
			time.Sleep(time.Second)
		}
	})()

	// Concurrently listens from s2.
	go s11.Listen()
	go s12.Listen()
	go s21.Listen()
	go s22.Listen()
	go s31.Listen()
	go s32.Listen()
	go s33.Listen()
	go Ebroker1.Listen()
	go Ebroker2.Listen()
	go Ebroker3.Listen()

	for {
		select {}
	}
}
