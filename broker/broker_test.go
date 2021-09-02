package broker

import (
	"fmt"
	"sync"
	"testing"
)

func TestBroker(t *testing.T) {
	b := newBroker()
	wg := sync.WaitGroup{}
	b.init(10)

	for i := 0; i < 10; i++{
		topic := fmt.Sprintf("topic:%d\n", i)
		payload := []byte(fmt.Sprintf("payload:%d\n", i))

		ch, err := b.subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)

		go func() {
			msg := <-ch
			if string(msg) != string(payload) {
				t.Fatalf("topic: %d, expected: %s, actual: %s\n", i,  string(payload), string(msg))
			}
			if err := b.unsubscribe(topic, ch); err != nil{
				t.Fatal(err)
			}
			wg.Done()
		}()

		if err := b.publish(topic, payload); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}