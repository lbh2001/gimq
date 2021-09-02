package broker

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"
)

// Broker interface
type Broker interface {
	publish(topic string, message []byte) error
	subscribe(topic string) (<-chan []byte, error)
	unsubscribe(topic string, sub <-chan []byte) error
	close()
	broadcast(message interface{}, subscribers []chan []byte)
	init(capacity int)
}

// broker is the internal broker
type broker struct {
	exit chan bool
	capacity  int
	topics	  map[string][]chan []byte
	sync.RWMutex
	mu    	  sync.RWMutex
	persisted map[string]bool	// if a topic has been persisted
}

// Message records a message's topic and timestamp
type Message struct {
	PayLoad		[]byte
	TimeStamp	time.Time
	Topic		string
}


// newBroker init a new broker
func newBroker() *broker{
	return &broker{
		exit: make(chan bool),
		capacity: 1,
		topics: make(map[string][]chan []byte),
		persisted: make(map[string]bool),
	}
}

func (b *broker) publish(topic string, message []byte) error{
	select {
		case <-b.exit:
			return errors.New("broker is closed")
	default:
	}
	b.RLock()
	subscribers, ok := b.topics[topic]
	b.RUnlock()
	if !ok {
		if err := b.persist(topic); err != nil {
			return err
		}
		return nil
	}
	b.broadcast(message, subscribers)
	return nil
}

func (b *broker) subscribe(topic string) (<-chan []byte, error){
	select {
	case <-b.exit:
		return nil, errors.New("broker is closed")
	default:
	}
	// make a new channel and append to list
	ch := make(chan []byte, b.capacity)
	b.Lock()
	b.topics[topic] = append(b.topics[topic], ch)
	b.Unlock()
	return ch, nil
}

func (b *broker) unsubscribe(topic string, sub <-chan []byte) error{
	select {
	case <-b.exit:
		return errors.New("broker is closed")
	default:
	}

	b.Lock()
	subscribers, ok := b.topics[topic]
	b.Unlock()
	if !ok {
		return nil
	}
	// find the subscriber need to delete in sub list
	var newSubs []chan []byte
	for _, subscriber := range subscribers{
		if subscriber == sub{
			continue
		}
		newSubs = append(newSubs, subscriber)
	}

	// replace sub list
	b.Lock()
	b.topics[topic] = newSubs
	b.Unlock()
	return nil
}

func (b *broker) close(){
	select {
	case <-b.exit:
		return
	default:
		close(b.exit)
		b.Lock()
		b.topics = make(map[string][]chan []byte)
		b.Unlock()
	}
}

func (b *broker) broadcast(message []byte, subscribers []chan []byte){
	n := len(subscribers)
	var concurrency int
	// determine concurrency based on subs' volume
	switch  {
	case n > 1000:
		concurrency = 3
	case n > 100:
		concurrency = 2
	default:
		concurrency = 1
	}
	// concurrent broadcast messages
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			for j := i; j < n; j += concurrency{
				select {
				// message is disseminated into channel
				case subscribers[j] <- message:
				// if timeout
				case <-time.After(time.Millisecond * 5):
				// if the broker is closed
				case <-b.exit:
					return
				}
			}
		}(i)
	}
}

// persist implements gimq's persistence
func(b *broker) persist(topic string) error{
	b.mu.Lock()
	defer b.mu.Unlock()
	// if it had been persisted
	if b.persisted[topic]{
		return nil
	}
	ch, err := b.subscribe(topic)
	if err != nil {
		return err
	}
	// open a file to save data
	f, err := os.OpenFile("gimq" + topic, os.O_CREATE | os.O_RDWR | os.O_APPEND, 0660)
	if err != nil{
		f.Close()
		return err
	}

	go func() {
		t := time.NewTicker(time.Second)
		var db []byte
		newLine := []byte{'\n'}
		defer t.Stop()
		defer f.Close()
		for{
			select {
			// read payload from channel
			case payload := <-ch:
				msg, err := json.Marshal(&Message{
					PayLoad: payload,
					TimeStamp: time.Now(),
					Topic: topic,
				})
				if err != nil{
					continue
				}
				db = append(db, msg...)
				db = append(db, newLine...)
			// if timeout
			case <-t.C:
				if len(db) == 0{
					continue
				}
				// If there are no new messages for a while,
				// the data in the db is saved to a file
				f.Write(db)
				db = nil
			// if broker is closed
			case <-b.exit:
				return
			}
		}
	}()
	b.persisted[topic] = true
	return nil
}

func (b *broker) init(capacity int){
	b.capacity = capacity
}
