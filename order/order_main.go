package main

import (
	"fmt"
	"math/rand"
	"time"

	"../edge_broker"
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

func vendorOrderPublisher(broker *edge_broker.Broker) {
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

func customerOrderPublisher(broker *edge_broker.Broker) {
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
	// construct new broker.
	broker := edge_broker.NewBroker()
	// create new subscriber
	s1 := broker.AddSubscriber()
	// subscribe BTC and ETH to s1.
	broker.Subscribe(s1, "VENDOR/CAR/HONDA")
	broker.Subscribe(s1, "VENDOR/3C/APPLE")
	// create new subscriber
	s2 := broker.AddSubscriber()
	// subscribe ETH and SOL to s2.
	broker.Subscribe(s2, "VENDOR/3C/APPLE")
	broker.Subscribe(s2, "VENDOR/APPLIANCES/LG")
	go (func() {
		// sleep for 5 sec, and then subscribe for topic DOT for s2
		time.Sleep(3 * time.Second)
		broker.Subscribe(s2, "VENDOR/CAR/TOYOTA")
	})()

	go (func() {
		// sleep for 5 sec, and then unsubscribe for topic SOL for s2
		time.Sleep(5 * time.Second)

		//broker.Unsubscribe(s2, "VENDOR/CAR/TOYOTA")
		//fmt.Printf("Total subscribers for topic TOYOTA is %v\n", broker.GetSubscribers("TOYOTA"))

		if broker.Unsubscribe(s2, "VENDOR/CAR/TOYOTA") {
			fmt.Printf("Total subscribers for topic TOYOTA is %v\n", broker.GetSubscribers("TOYOTA"))
		}
		if broker.Unsubscribe(s2, "VENDOR/CAR/BMW") {
			fmt.Printf("Total subscribers for topic BMW is %v\n", broker.GetSubscribers("BMW"))
		}
		if broker.Unsubscribe(s1, "VENDOR/CAR/BMW") {
			fmt.Printf("Total subscribers for topic BMW is %v\n", broker.GetSubscribers("BMW"))
		}
	})()

	go (func() {
		// sleep for 5 sec, and then unsubscribe for topic SOL for s2
		time.Sleep(10 * time.Second)
		broker.RemoveSubscriber(s2)
		fmt.Printf("RemoveSubscriber: Total subscribers for topic APPLE is %v\n", broker.GetSubscribers("VENDOR/3C/APPLE"))
	})()

	// Concurrently publish the values.
	go vendorOrderPublisher(broker)
	//go customerOrderPublisher(broker)
	// Concurrently listens from s1.
	go s1.Listen()
	// Concurrently listens from s2.
	go s2.Listen()
	// to prevent terminate
	fmt.Scanln()
	fmt.Println("Done!")
}
