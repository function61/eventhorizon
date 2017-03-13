package server

import (
	"bufio"
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/pubsub"
	"github.com/function61/pyramid/pubsub/partitionedlossyqueue"
	"log"
	"net"
	"time"
)

// client from the server's perspective
type ServerClient struct {
	Addr                  string
	authenticated         bool
	disconnected          bool
	closeCh               chan bool
	writeCh               chan string
	subscriptionsByClient []string
	sendQueue             *partitionedlossyqueue.Queue
	conn                  net.Conn
}

func (c *ServerClient) DisconnectNonBlocking() {
	select {
	case c.closeCh <- true:
	// noop
	default:
		// close request already in flight
	}
}

type ClientsBySubscription map[string][]*ServerClient

type IncomingMessage struct {
	message []string
	client  *ServerClient
}

type PubSubServer struct {
	clientBySubscription    ClientsBySubscription
	listener                net.Listener
	acceptorAndMainLoopDone chan bool
	stopMainLoop            chan bool
	clientDisconnected      chan *ServerClient
	messageReceived         chan *IncomingMessage
}

func New(bindAddr string) *PubSubServer {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		panic(err)
	}

	log.Printf("PubSubServer: binding to %s", bindAddr)

	e := &PubSubServer{
		clientBySubscription:    make(ClientsBySubscription),
		listener:                listener,
		acceptorAndMainLoopDone: make(chan bool),
		stopMainLoop:            make(chan bool),
		clientDisconnected:      make(chan *ServerClient, 100),
		messageReceived:         make(chan *IncomingMessage, 1000),
	}

	go e.acceptorLoop(listener.(*net.TCPListener))
	go e.mainLogicLoop()

	return e
}

func (e *PubSubServer) Close() {
	log.Printf("PubSubServer: stopping")

	e.listener.Close()

	e.stopMainLoop <- true

	<-e.acceptorAndMainLoopDone
	<-e.acceptorAndMainLoopDone

	log.Printf("PubSubServer: stopped")
}

func (e *PubSubServer) writeForOneClient(cl *ServerClient) {
	for {
		select {
		case <-cl.closeCh:
			// this could be called many times but it doesn't matter.
			// and there's not much we could do on error closing the connection
			cl.conn.Close()
			return
		case msgToWrite := <-cl.writeCh:
			if _, err := cl.conn.Write([]byte(msgToWrite)); err != nil {
				e.clientDisconnected <- cl
				return
			}
		case <-cl.sendQueue.ReceiveAvailable:
			packet := ""
			for _, message := range cl.sendQueue.ReceiveAndClear() {
				packet += message
			}

			if _, err := cl.conn.Write([]byte(packet)); err != nil {
				e.clientDisconnected <- cl
				return
			}
		}
	}
}

func (e *PubSubServer) handleSubscribe(topic string, cl *ServerClient) {
	log.Printf("PubSubServer: subscribe; topic=%s", topic)

	cl.subscriptionsByClient = append(cl.subscriptionsByClient, topic)

	e.clientBySubscription[topic] = append(e.clientBySubscription[topic], cl)
}

func (e *PubSubServer) readFromOneClient(cl *ServerClient) {
	reader := bufio.NewReader(cl.conn)

	// read until we receive a disconnect
	for {
		rawMessage, errRead := reader.ReadString('\n')
		if errRead != nil {
			// we're going to come here also if TCP keepalive times out
			e.clientDisconnected <- cl
			return
		}

		// 'SET key value\n' => [ 'SET', 'key', 'value' ]
		msgParts := pubsub.MsgformatDecode(rawMessage)

		e.messageReceived <- &IncomingMessage{
			message: msgParts,
			client:  cl,
		}
	}
}

// most of the logic runs in this goroutine. we do all data manipulation here
// so there's no need to acquire locks
func (e *PubSubServer) mainLogicLoop() {
	log.Printf("PubSubServer: starting mainLogicLoop")
	defer func() { e.acceptorAndMainLoopDone <- true }()

	for {
		select {
		case <-e.stopMainLoop:
			return
		case incomingMessage := <-e.messageReceived:
			// increase readability
			client := incomingMessage.client
			msg := incomingMessage.message

			// no need to assert len(msgParts) > 0 because
			// MsgformatDecode() guarantees at least one part
			msgType := msg[0]

			if msgType == "PUB" && len(msg) == 3 {
				if !client.authenticated {
					log.Printf("PubSubServer: attempt to invoke privileged action while unauthorized")
					client.DisconnectNonBlocking()
					break // to select
				}

				topic := msg[1]
				message := msg[2]

				notifyMsg := pubsub.MsgformatEncode([]string{"NOTIFY", topic, message})

				// OK if subscription does not exist
				for _, subscriberClient := range e.clientBySubscription[topic] {
					// guaranteed to never block, but can lose all but the latest message per topic
					subscriberClient.sendQueue.Put(topic, notifyMsg)
				}
			} else if msgType == "SUB" && len(msg) == 2 {
				if !client.authenticated {
					log.Printf("PubSubServer: attempt to invoke privileged action while unauthorized")
					client.DisconnectNonBlocking()
					break // to select
				}

				topic := msg[1]

				e.handleSubscribe(topic, incomingMessage.client)

				client.conn.Write([]byte(pubsub.MsgformatEncode([]string{"OK"})))
			} else if msgType == "AUTH" && len(msg) == 2 {
				if msg[1] == config.AUTH_TOKEN {
					client.authenticated = true
				}
			} else if msgType == "BYE" && len(msg) == 1 {
				client.DisconnectNonBlocking()
				break // to select
			} else {
				log.Printf("Unsupported message type: %s", msgType)
				client.DisconnectNonBlocking()
			}
		case client := <-e.clientDisconnected:
			// message may arrive multiple times per client
			if !client.disconnected {
				client.disconnected = true

				log.Printf("PubSubServer: socket disconnected")

				e.removeClientSubscriptions(client)
			}
		}
	}
}

func (e *PubSubServer) acceptorLoop(listener *net.TCPListener) {
	log.Printf("PubSubServer: starting acceptorLoop")
	defer func() { e.acceptorAndMainLoopDone <- true }()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			// not much sense in doing anything with error, unless we can distinquish
			// the error from the error triggered by listener.Close() which gets
			// called when our server Close() is called
			break
		}

		// enable keepalive, so broken connections are cleaned up
		if err := conn.SetKeepAlivePeriod(60 * time.Second); err != nil {
			log.Printf("PubSubServer: unable to set keepalive period: %s", err.Error())
		}
		if err := conn.SetKeepAlive(true); err != nil {
			log.Printf("PubSubServer: unable to enable keepalive: %s", err.Error())
		}

		log.Printf("PubSubServer: accepted connection from %s", conn.RemoteAddr())

		cl := ServerClient{
			Addr:                  conn.RemoteAddr().String(),
			writeCh:               make(chan string, 5),
			closeCh:               make(chan bool, 1),
			subscriptionsByClient: []string{},
			sendQueue:             partitionedlossyqueue.New(),
			conn:                  conn,
		}

		go e.writeForOneClient(&cl)
		go e.readFromOneClient(&cl)
	}

}

func (e *PubSubServer) removeClientSubscriptions(cl *ServerClient) {
	for _, topic := range cl.subscriptionsByClient {

		foundIndex := -1
		for idx, value := range e.clientBySubscription[topic] { // find position of element
			if value == cl {
				foundIndex = idx
				break
			}
		}

		if foundIndex != -1 { // delete by position
			log.Printf("PubSubServer: removeClientSubscriptions: removing subscription. topic=%s pos=%d", topic, foundIndex)

			temp := e.clientBySubscription[topic]

			// log.Printf("Before %v", temp)

			temp = append(
				temp[:foundIndex],
				temp[foundIndex+1:]...)

			e.clientBySubscription[topic] = temp

			// log.Printf("After %v", temp)
		} else {
			log.Printf("PubSubServer: removeClientSubscriptions: sub not found. SHOULD NOT HAPPEN. topic=%s", topic)
		}
	}
}
