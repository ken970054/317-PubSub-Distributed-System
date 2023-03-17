package brokers

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

type B_Subscriptions map[string]interface{} // could be subscribed from

type MasterBroker struct {
	brokersList []*Broker // list for all brokers
	//edgeTopics  map[string]bool			// manage subscribed topic from all edge brokers
	edgeBrokerTopics   map[string]map[string]bool // manage subscribed topic from all edge brokers
	masterBrokerTopics map[string]B_Subscriptions // Master broker topics(from MQTT) for edge brokers to subscribe
	messages           chan *Message
	active             bool
	mut                sync.RWMutex // mutex lock
}

func NewMasterBroker(chanSize int) *MasterBroker {
	// returns new broker object
	return &MasterBroker{
		brokersList:        []*Broker{},
		edgeBrokerTopics:   map[string]map[string]bool{},
		masterBrokerTopics: map[string]B_Subscriptions{},
		messages:           make(chan *Message, chanSize),
		active:             true,
	}
}

// ////////////////////////
// Work as a subscriber //
// ////////////////////////
func (MB *MasterBroker) AddEdgeTopic(EB_id string, topic string) {
	MB.mut.RLock()
	defer MB.mut.RUnlock()
	if MB.edgeBrokerTopics[EB_id] == nil {
		MB.edgeBrokerTopics[EB_id] = map[string]bool{}
	}
	MB.edgeBrokerTopics[EB_id][topic] = true // make sure ED_id exists (guarantee by Subscribe function)
}

func (MB *MasterBroker) RemoveEdgeTopic(EB_id string, topic string) {
	MB.mut.Lock()
	defer MB.mut.Unlock()
	delete(MB.edgeBrokerTopics[EB_id], topic)
}

func (MB *MasterBroker) Signal(msg *Message) {
	// Get the message from the channel
	MB.mut.RLock()
	defer MB.mut.RUnlock()
	if MB.active {
		MB.messages <- msg
	}
}

func (MB *MasterBroker) CheckMessage() {
	MB.mut.RLock()
	defer MB.mut.RUnlock()
	if MB.active {
		fmt.Printf("There are %d message in Master Broker\n", len(MB.messages))
	}
}

// // Need to pass mqttServer as an input and call its Publish function
func (MB *MasterBroker) Publish2MQTT(MQ *MQTT) {
	// Publish message that subscribed from edge broker to Mosquitto broker(ex: from order side to package side)
	for {
		if msg, ok := <-MB.messages; ok {
			encodeMsg, _ := json.Marshal(msg.GetData())
			err := MQ.Publish(MB.Topic4Publishing(msg.GetTopic()), encodeMsg)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("Master broker send encoded message to Mosquitto broker in topic '%s'\n", MB.Topic4Publishing(msg.GetTopic()))
		}
	}
}

func (MB *MasterBroker) Publish2EdgeBroker(respDataChan chan string) {
	for {
		// Publish message that subscribed from Mosquitto broker to edge broker (ex: from other side to current side)
		if respData, ok := <-respDataChan; ok {
			fmt.Printf("Got response %s \n", respData)
			// split topic and message from MQTT
			splitRespData := strings.Split(respData, "@")
			topic := splitRespData[0]

			// to delete the second region of topic
			splits := strings.Split(topic, "/")
			processedTopic := ""
			for i, s := range splits {
				if i != 1 {
					processedTopic = processedTopic + s
				}
				if i != 1 && i != len(splits)-1 {
					processedTopic = processedTopic + "/"
				}
			}
			// publish to all edge brokers that has subscribe to Master broker
			MB.Publish(processedTopic, splitRespData[1], map[string]string{})
		}
	}
}

// ////////////////////////////////////////////////
// Work as broker for edge brokers to subscribe //
// ////////////////////////////////////////////////
func (MB *MasterBroker) Subscribe(s interface{}, topic string) {
	MB.mut.Lock()
	defer MB.mut.Unlock()

	if MB.masterBrokerTopics[topic] == nil {
		MB.masterBrokerTopics[topic] = B_Subscriptions{}
	}

	switch s := s.(type) {
	// if subscribe from local client subscriber(special case)
	case *Subscriber:
		s.AddTopic(topic)
		MB.masterBrokerTopics[topic][s.id] = s
		fmt.Printf("Client %s subscribed for topic: %s\n", s.id, topic)
	// if subscribe from edge broker (regular case)
	case *Broker:
		s.AddMasterTopic(topic)
		MB.masterBrokerTopics[topic][s.id] = s
		fmt.Printf("Edge broker %s subscribe topic '%s' to Master broker\n", s.id, topic)
	}
}

func (MB *MasterBroker) Unsubscribe(s interface{}, topic string) bool {
	MB.mut.Lock()
	defer MB.mut.Unlock()

	if MB.masterBrokerTopics[topic] == nil {
		fmt.Printf("Topic %s hasn't been subscribed before by edge broker.", topic)
		return false
	} else {
		switch s := s.(type) {
		// if unsubscribe from local client subscriber(special case)
		case *Subscriber:
			if _, existed := MB.masterBrokerTopics[topic][s.id]; existed {
				delete(MB.masterBrokerTopics[topic], s.id)
				s.RemoveTopic(topic)
				fmt.Printf("Edge broker %s unsubscribed for topic: %s\n", s.id, topic)
				return true
			}
			fmt.Printf("Edge broker %s unsubscribed for topic: %s\n", s.id, topic)
			return false
		// if unsubscribe from edge broker (regular case)
		case *Broker:
			if _, existed := MB.masterBrokerTopics[topic][s.id]; existed {
				delete(MB.masterBrokerTopics[topic], s.id)
				s.RemoveMasterTopic(topic)
				fmt.Printf("Edge broker %s unsubscribed for topic: %s\n", s.id, topic)
				return true
			}
			fmt.Printf("Edge broker %s unsubscribed for topic: %s\n", s.id, topic)
			return false
		}
	}
	return false
}

func (MB *MasterBroker) Publish(topic string, msg string, attributes map[string]string) {
	// publish the message to given topic, so the subscribers who have subscribed the topic will update the message
	publishTime := time.Now().UTC().Format("2006-01-02 15:04:05")
	//fmt.Printf(publishTime)
	MB.mut.RLock()
	defer MB.mut.RUnlock()
	MBTopics := MB.masterBrokerTopics[topic]

	for _, s := range MBTopics {
		m := NewMessage(topic, msg, attributes, publishTime)
		switch s := s.(type) {
		case *Subscriber: //(special case)
			if !s.active {
				fmt.Printf("Subscriber has been deleted\n")
				return
			}
			go (func(s *Subscriber) {
				fmt.Printf("Publish to Local Client!\n")
				s.Signal(m)
				//s.CheckMessage()
			})(s)
		case *Broker:
			if s.status != 1 {
				fmt.Printf("Master broker Subscriber has been deleted.\n")
			}
			go (func(s *Broker) {
				fmt.Printf("Publish to Edge Broker with %s topic!\n", topic)
				s.Signal(m)
			})(s)
		}
	}
}

// ///////////////////////////////////////
// Topic and Message processing for MQTT//
// ///////////////////////////////////////
var Client_Side = "SHIPPING"

func (MB *MasterBroker) Topic4Publishing(localTopic string) string {
	return Client_Side + "/" + localTopic
}

//func (MB *MasterBroker) ReceivingTopic(globalTopic string) {
//
//}

// ///////////////////////
// Manage Edge Brokers //
// ///////////////////////

//func ManageEdgeBrokers() Instruction {

//}

//func DetectSource() {

//}

//func (MB *MasterBroker) Allocate(B_1 *Broker, B_2 *Broker) {

//}

// return topics that subscribed from the given edge id
func (MB *MasterBroker) GetEdgeTopics(EB_id string) []string {
	MB.mut.RLock()
	defer MB.mut.RUnlock()
	topics := []string{}
	for topic, _ := range MB.edgeBrokerTopics[EB_id] {
		topics = append(topics, topic)
	}
	return topics
}
