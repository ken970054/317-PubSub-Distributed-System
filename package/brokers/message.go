package brokers

import (
	"sync"
)

type Message struct {
	topic       string
	data        string
	attributes  map[string]string
	publishTime string // could be used for message ordering
	//messageId string  // ID of this message, assigned by the server when the message is published. Guaranteed to be unique within the topic.

	mutex sync.RWMutex
}

func NewMessage(topic string, msg string, attributes map[string]string, time string) *Message {
	// Returns the message object
	return &Message{
		topic:       topic,
		data:        msg,
		attributes:  attributes,
		publishTime: time,
	}
}

func (m *Message) GetTopic() string {
	// returns the topic of the message
	return m.topic
}

func (m *Message) GetPublishTime() string {
	return m.publishTime
}

func (m *Message) GetData() string {
	return m.data
}

func (m *Message) GetAttributes() map[string]string {
	return m.attributes
}
