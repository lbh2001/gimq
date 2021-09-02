package broker

type Client struct {
	bk *broker
}

// NewClient init a new Client
func NewClient() *Client{
	return &Client{
		bk: newBroker(),
	}
}

func (c *Client) Publish(topic string, message []byte) error{
	return c.bk.publish(topic, message)
}

func (c *Client) Subscribe(topic string) (<-chan []byte, error){
	return c.bk.subscribe(topic)
}

func (c *Client) Unsubscribe(topic string, sub <-chan []byte) error{
	return c.bk.unsubscribe(topic, sub)
}

func (c *Client) Close(){
	c.bk.close()
}

//func (c *Client) Broadcast(message []byte, subscribers []chan []byte){
//	c.bk.broadcast(message, subscribers)
//}

func (c *Client) Init(capacity int){
	c.bk.init(capacity)
}