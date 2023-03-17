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
	////////////////////////////////////////////////////////////////
	// Construct Mosquitto and connection with client-side server //
	////////////////////////////////////////////////////////////////
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
	s1 := Ebroker1.AddSubscriber(10)
	s2 := Ebroker2.AddSubscriber(10)

	// subscribe: master broker -> edge broker //
	/* 1. master broker can subscribe to all edge brokers
	 * 2. Subscribed topic format: starts from the name of other client-side servers so as to identify where the message will send to
	 */
	Ebroker1.Subscribe(Mbroker, "ORDER/CAR/HONDA")
	Ebroker2.Subscribe(Mbroker, "PACKAGE/3C/APPLE")
	Ebroker3.Subscribe(Mbroker, "PACKAGE/3C/APPLE")
	Ebroker3.Subscribe(Mbroker, "ORDER/3C/APPLE")

	Ebroker3.Unsubscribe(Mbroker, "PACKAGE/3C/APPLE")
	attributeAA := map[string]string{}
	Ebroker2.Publish("PACKAGE/CAR/HONDA", "MB -> EB", attributeAA)

	// subscribe: edge broker -> master broker
	/* 1. all edge brokers can subscribe to master broker
	 * 2. Subscribed topic format: starts from the name of the other client-side servers to receive and then process where the messages come from and the following content refer to the details of other client-side info
	 */
	Mbroker.Subscribe(Ebroker1, "SHIPPING/3C/APPLE/STATUS")
	Mbroker.Subscribe(Ebroker2, "NOTIFICATION/CUSTOMER/CAR/TOYOTA/STATUS")
	Mbroker.Subscribe(Ebroker3, "NOTIFICATION/VENDOR/CAR/HONDA/STATUS")

	//Mbroker.Publish("SHIPPING/3C/APPLE/STATUS", "EB -> MB, then send to MQTT", attributeAA)
	// subscribe: local client -> edge broker //
	/* 1. one local client only connect to one edge broker, but can subscribe multiple topics
	 * 2. Subscribed topic format: starts from the name of the current client-side server so as to identify where
	 */
	Ebroker1.Subscribe(s1, "PACKAGE/VENDOR/CAR/HONDA/STATUS")
	Ebroker1.Subscribe(s1, "NOTIFICATION/VENDOR/3C/APPLE/STATUS")
	Ebroker2.Subscribe(s2, "NOTIFICATION/CUSTOMER/CAR/TOYOTA/STATUS")
	Ebroker2.Subscribe(s2, "NOTIFICATION/VENDOR/APPLIANCES/LG/STATUS")

	Ebroker1.Subscribe(s1, "VENDOR/CAR/HONDA")

	go (func() {
		// sleep for 5 sec, and then subscribe for topic DOT for s2
		time.Sleep(3 * time.Second)
		Ebroker2.Subscribe(s2, "VENDOR/CAR/TOYOTA")
		Ebroker2.Subscribe(Mbroker, "PACKAGE/APPLIANCES/SONY")

		//fmt.Println(Mbroker.GetEdgeTopics(Ebroker1.id))
	})()

	go (func() {
		// sleep for 5 sec, and then unsubscribe for topic SOL for s2
		time.Sleep(5 * time.Second)

		if Ebroker2.Unsubscribe(s2, "VENDOR/CAR/TOYOTA") {
			fmt.Printf("Total subscribers for topic TOYOTA is %v\n", Ebroker2.GetSubscribers("TOYOTA"))
		}
		if Ebroker2.Unsubscribe(s2, "VENDOR/CAR/BMW") {
			fmt.Printf("Total subscribers for topic BMW is %v\n", Ebroker2.GetSubscribers("BMW"))
		}
		if Ebroker1.Unsubscribe(s1, "VENDOR/CAR/BMW") {
			fmt.Printf("Total subscribers for topic BMW is %v\n", Ebroker1.GetSubscribers("BMW"))
		}
		if Ebroker2.Unsubscribe(Mbroker, "PACKAGE/APPLIANCES/SONY") {
			fmt.Printf("Total subscribers for topic PACKAGE/APPLIANCES/SONY is %v\n", Ebroker2.GetSubscribers("PACKAGE/APPLIANCES/SONY"))
		}
	})()

	go (func() {
		// sleep for 5 sec, and then unsubscribe for topic SOL for s2
		time.Sleep(10 * time.Second)
		Ebroker2.RemoveSubscriber(s2)
		fmt.Printf("RemoveSubscriber: Total subscribers for topic APPLE is %v\n", Ebroker2.GetSubscribers("VENDOR/3C/APPLE"))
	})()

	//// Concurrently publish the values.
	go vendorOrderPublisher(Ebroker1)
	go customerOrderPublisher(Ebroker2)

	//// Concurrently publish the messages to Mosquitto
	//go (func() {
	//	message, _ := json.Marshal("String to be encoded")
	//	err := mqttService.Publish("SHIPPING/3C/APPLE/STATUS", message)
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//	time.Sleep(time.Second)
	//})()
	go Mbroker.Publish("NOTIFICATION/ORDER/CAR/HONDA", "Pub from MB msg", map[string]string{})
	go Mbroker.Publish2MQTT(mqttService)

	respDataChan := make(chan []byte)
	go func() {
		mqttService.Subscribe("ORDER/PACKAGE/CAR/HONDA", respDataChan)
		if respData, ok := <-respDataChan; ok {
			fmt.Printf("Got response %v \n", string(respData))
		}
	}()

	//// Concurrently listens from s1.
	go s1.Listen()

	//// publish to s1 and chech the message channel
	attributes := map[string]string{
		"items":    "item name",
		"price":    "xxx",
		"quantity": "zzz",
	}
	//Ebroker1.Publish("VENDOR/CAR/HONDA", "For test1", attributes)
	//Ebroker1.Publish("VENDOR/CAR/HONDA", "For test2", attributes)
	Ebroker1.Publish("ORDER/CAR/HONDA", "From NOTIFICATION to ORDER", attributes)

	// Concurrently listens from s2.
	go s2.Listen()
	go Ebroker2.Listen()
	go Ebroker1.Listen()
	// to prevent terminate
	//fmt.Scanln()
	//fmt.Println("Done!")

	for {
		select {}
	}
}
