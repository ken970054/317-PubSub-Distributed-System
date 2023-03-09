package order_pubsub

import (
	"fmt"
	"sync"
	"time"
)

type Subscriptions map[string]*Subscriber

type Broker struct {
	subscriptions Subscriptions            // map of subscribers id:Subscriber for the subscriptions
	topics        map[string]Subscriptions // map of topic to subscriptions
	mut           sync.RWMutex             // mutex lock
}

func NewBroker() *Broker {
	// returns new broker object
	return &Broker{
		subscriptions: Subscriptions{},
		topics:        map[string]Subscriptions{},
	}
}

func (b *Broker) AddSubscriber() *Subscriber {
	// Add subscriber to the Subscription of broker
	b.mut.Lock()
	defer b.mut.Unlock()
	id, s := CreateNewSubscriber()
	b.subscriptions[id] = s
	return s
}

func (b *Broker) RemoveSubscriber(s *Subscriber) {
	// remove subscriber to the Subscription of broker
	//unsubscribe to all topics which s is subscribed to.

	// add mutex RLock for iterating s.topics (Publish() might assign data to the removing subscriber)
	s.mutex.RLock()
	stopics := s.topics
	s.mutex.RUnlock()

	//for topic := range s.topics {
	for topic := range stopics {
		b.Unsubscribe(s, topic)
	}
	b.mut.Lock()
	// remove subscriber from list of subscriptions
	delete(b.subscriptions, s.id)
	b.mut.Unlock()
	s.Destruct()
}

func (b *Broker) Broadcast(msg string, topics []string, attributes map[string]string) {
	// broadcast message to all topics
	for _, topic := range topics {
		for _, s := range b.topics[topic] {
			publishTime := time.Now().Format("2006-01-02 15:04:05")
			m := NewMessage(topic, msg, attributes, publishTime)
			go (func(s *Subscriber) {
				s.Signal(m)
			})(s)
		}
	}
}

func (b *Broker) GetSubscribers(topic string) int {
	// get total subscribers subscribed to given topic.
	b.mut.RLock()
	defer b.mut.RUnlock()
	return len(b.topics[topic])
}

func (b *Broker) Subscribe(s *Subscriber, topic string) {
	// subscribe to given topic
	b.mut.Lock()
	defer b.mut.Unlock()
	// if there is no such topic in broker subscription, create a new one
	if b.topics[topic] == nil {
		b.topics[topic] = Subscriptions{}
	}
	s.AddTopic(topic)
	b.topics[topic][s.id] = s
	fmt.Printf("%s Subscribed for topic: %s\n", s.id, topic)
}

func (b *Broker) Unsubscribe(s *Subscriber, topic string) bool {
	// unsubscribe to given topic
	//b.mut.RLock()
	//defer b.mut.RUnlock()
	// chage to write lock
	b.mut.Lock()
	defer b.mut.Unlock()

	//delete(b.topics[topic], s.id)
	//s.RemoveTopic(topic)
	//fmt.Printf("%s Unsubscribed for topic: %s\n", s.id, topic)

	if b.topics[topic] == nil {
		fmt.Printf("Topic %s hasn't been subscribed before\n", topic)
		return false
	} else {

		if _, existed := b.topics[topic][s.id]; existed {
			delete(b.topics[topic], s.id) // delete subscriber from topic of broker
			s.RemoveTopic(topic)
			fmt.Printf("%s Unsubscribed for topic: %s\n", s.id, topic)
			return true
		}

		fmt.Printf("Subscriber %s hasn't subscribed to topic %s\n", s.id, topic)
		return false
	}
}

// /////////////////////////
// // Publish to broker ////
// /////////////////////////
func (b *Broker) Publish(topic string, msg string, attributes map[string]string) {
	// publish the message to given topic, so the subscribers who have subscribed the topic will update the message
	publishTime := time.Now().Format("2006-01-02 15:04:05")
	b.mut.RLock()
	defer b.mut.RUnlock()
	bTopics := b.topics[topic]
	//b.mut.RUnlock()

	for _, s := range bTopics {
		m := NewMessage(topic, msg, attributes, publishTime)
		if !s.active {
			fmt.Printf("Subscriber has been deleted\n")
			return
		}
		go (func(s *Subscriber) {
			s.Signal(m)
		})(s)
	}
}
