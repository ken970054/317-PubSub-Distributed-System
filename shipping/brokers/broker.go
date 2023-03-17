package brokers

import (
	"crypto/rand"
	"fmt"
	"log"
	"sync"
	"time"
)

type Subscriptions map[string]interface{} // replace inferface{} to fixed type as the value, it allows value to be any type

type Broker struct {
	id            string // id of edge broker
	messages      chan *Message
	subscriptions Subscriptions            // map of subscribers id:Subscriber for the subscriptions
	topics        map[string]Subscriptions // map of topic to subscriptions
	masterTopics  map[string]bool          // topics that subscribed from master broker
	status        int                      // working status of edge broker, -1 = breakdown, 0 = shut down, 1 = working
	mut           sync.RWMutex             // mutex lock
}

func NewBroker(chanSize int) *Broker {
	// returns new broker object
	// create id for edge broker
	b := make([]byte, 4)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	id := fmt.Sprintf("B%X", b[0:4])
	fmt.Printf("Create new Broker id: %s\n", id)
	return &Broker{
		id:            id,
		messages:      make(chan *Message, chanSize),
		subscriptions: Subscriptions{},
		topics:        map[string]Subscriptions{},
		masterTopics:  map[string]bool{},
		status:        1,
	}
}

func (b *Broker) GetBrokerId() string {
	return b.id
}

func (b *Broker) AddSubscriber(chanSize int) *Subscriber {
	// Add subscriber to the Subscription of broker
	b.mut.Lock()
	defer b.mut.Unlock()
	id, s := CreateNewSubscriber(chanSize)
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

// //////////////////////////
// Work as an edge broker //
// //////////////////////////
func (b *Broker) Broadcast(msg string, topics []string, attributes map[string]string) {
	// broadcast message to all topics
	for _, topic := range topics {
		for _, s := range b.topics[topic] {
			publishTime := time.Now().UTC().Format("2006-01-02 15:04:05")
			m := NewMessage(topic, msg, attributes, publishTime)
			switch s := s.(type) {
			case *Subscriber:
				go (func(s *Subscriber) {
					s.Signal(m)
				})(s)
			case *MasterBroker:
				go (func(s *MasterBroker) {
					s.Signal(m)
				})(s)
			}
		}
	}
}

func (b *Broker) GetSubscribers(topic string) int {
	// get total subscribers subscribed to given topic.
	b.mut.RLock()
	defer b.mut.RUnlock()
	return len(b.topics[topic])
}

// // allow to subscribe the given topic from client and master broker ////
func (b *Broker) Subscribe(s interface{}, topic string) {
	b.mut.Lock()
	defer b.mut.Unlock()

	// if there is no such topic in broker subscription, create a new one
	if b.topics[topic] == nil {
		b.topics[topic] = Subscriptions{}
	}

	switch s := s.(type) {
	// if the type of subscriber is Subscriber(client)
	case *Subscriber:
		s.AddTopic(topic)
		b.topics[topic][s.id] = s
		fmt.Printf("Client %s subscribed for topic: %s\n", s.id, topic)
		// if the type of subscriber is MasterBroker, now s refers to element of master broker
	case *MasterBroker:
		s.AddEdgeTopic(b.id, topic)
		b.topics[topic]["MB"] = s // only one master broker, so give an unique name "MB" to represent
		fmt.Printf("Master Broker subscribed for topic: %s\n", topic)
	}
}

func (b *Broker) Unsubscribe(s interface{}, topic string) bool {
	// unsubscribe to given topic
	// chage to write lock
	b.mut.Lock()
	defer b.mut.Unlock()

	if b.topics[topic] == nil {
		fmt.Printf("Topic %s hasn't been subscribed before\n", topic)
		return false
	} else {
		switch s := s.(type) {
		case *Subscriber:
			if _, existed := b.topics[topic][s.id]; existed {
				delete(b.topics[topic], s.id) // delete subscriber from topic of broker
				s.RemoveTopic(topic)
				fmt.Printf("Client %s unsubscribed for topic: %s\n", s.id, topic)
				return true
			}
			fmt.Printf("Subscriber %s hasn't subscribed to topic %s\n", s.id, topic)
			return false
		case *MasterBroker:
			if _, existed := b.topics[topic]["MB"]; existed {
				delete(b.topics[topic], "MB") // delete subscriber(Master broker) from topic of broker
				s.RemoveEdgeTopic(b.id, topic)
				fmt.Printf("Master Broker unsubscribed for topic: %s\n", topic)
				return true
			}
			fmt.Printf("Master broker hasn't subscribed to topic %s in edge broker %s\n", topic, b.id)
			return false
		}
	}
	return false
}

// /////////////////////////
// // Publish to broker ////
// /////////////////////////
func (b *Broker) Publish(topic string, msg string, attributes map[string]string) {
	// publish the message to given topic, so the subscribers who have subscribed the topic will update the message
	publishTime := time.Now().UTC().Format("2006-01-02 15:04:05")
	//fmt.Printf(publishTime)
	b.mut.RLock()
	defer b.mut.RUnlock()
	bTopics := b.topics[topic]
	//b.mut.RUnlock()

	for _, s := range bTopics {
		m := NewMessage(topic, msg, attributes, publishTime)
		switch s := s.(type) {
		case *Subscriber:
			if !s.active {
				fmt.Printf("Subscriber has been deleted\n")
				return
			}
			go (func(s *Subscriber) {
				fmt.Printf("Publish to Local Client!\n")
				s.Signal(m)
				//s.CheckMessage()
			})(s)
		case *MasterBroker:
			if !s.active {
				fmt.Printf("Master broker Subscriber has been deleted.\n")
			}
			go (func(s *MasterBroker) {
				fmt.Printf("Publish to Master Broker!\n")
				s.Signal(m)
			})(s)
		}
	}
}

////////////////////////
// Work as subscriber //
////////////////////////

func (b *Broker) AddMasterTopic(topic string) {
	b.mut.RLock()
	defer b.mut.RUnlock()
	b.masterTopics[topic] = true
}

func (b *Broker) RemoveMasterTopic(topic string) {
	b.mut.Lock()
	defer b.mut.Unlock()
	delete(b.masterTopics, topic)
}

func (b *Broker) GetMasterTopics() []string {
	b.mut.RLock()
	defer b.mut.RUnlock()
	topics := []string{}
	for topic, _ := range b.topics {
		topics = append(topics, topic)
	}
	return topics
}

func (b *Broker) Signal(msg *Message) {
	b.mut.RLock()
	defer b.mut.RUnlock()
	if b.status == 1 {
		b.messages <- msg
	}
}

func (b *Broker) CheckMessage() {
	b.mut.RLock()
	defer b.mut.RUnlock()
	if b.status == 1 {
		fmt.Printf("There are %d message in Master Broker\n", len(b.messages))
	}
}

func (b *Broker) Listen() {
	for {
		if msg, ok := <-b.messages; ok {
			fmt.Printf("Publish this message '%s' to edge broker immediately\n", msg.GetData())
		}
	}
}
