package server

import (
	"bufio"
	"fmt"
	"github.com/function61/pyramid/config"
	"github.com/function61/pyramid/pubsub"
	"github.com/function61/pyramid/pubsub/partitionedlossyqueue"
	"io"
	"log"
	"net"
	"syscall"
)

// client from the server's perspective
type ServerClient struct {
	Addr                  string
	writeCh               chan string
	subscriptionsByClient []string
	sendQueue             *partitionedlossyqueue.Queue
}

type ClientsBySubscription map[string][]*ServerClient

type PubSubServer struct {
	clientBySubscription ClientsBySubscription
	listener             net.Listener
	acceptorDone         chan bool
}

func New(bindAddr string) *PubSubServer {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		panic(err)
	}

	log.Printf("PubSubServer: binding to %s", bindAddr)

	e := &PubSubServer{
		clientBySubscription: make(ClientsBySubscription),
		listener:             listener,
		acceptorDone:         make(chan bool),
	}

	go e.acceptorLoop(listener)

	return e
}

func (e *PubSubServer) Close() {
	log.Printf("PubSubServer: closing. Stopping acceptorLoop.")

	e.listener.Close()

	<-e.acceptorDone

	log.Printf("PubSubServer: acceptor shut down")
}

func (e *PubSubServer) handleClientDisconnect(cl *ServerClient) {
	e.removeClientSubscriptions(cl)
}

func (e *PubSubServer) writeForOneClient(cl *ServerClient, conn net.Conn) {
	for {
		select {
		case msgToWrite := <-cl.writeCh:
			if _, err := conn.Write([]byte(msgToWrite)); err != nil {
				log.Printf("PubSubServer: write error to client %s. Stopping writer.", cl.Addr)
				e.handleClientDisconnect(cl)
				return
			}
		case <-cl.sendQueue.ReceiveAvailable:
			packet := ""
			for _, message := range cl.sendQueue.ReceiveAndClear() {
				packet += message
			}

			if _, err := conn.Write([]byte(packet)); err != nil {
				log.Printf("PubSubServer: write error to client %s. Stopping writer.", cl.Addr)
				e.handleClientDisconnect(cl)
				return
			}
		}
	}
}

func (e *PubSubServer) handlePublish(topic string, message string, cl *ServerClient) {
	notifyMsg := pubsub.MsgformatEncode([]string{"NOTIFY", topic, message})

	for _, subscriberClient := range e.clientBySubscription[topic] {
		// guaranteed to never block, but can lose all but the latest message per topic
		subscriberClient.sendQueue.Put(topic, notifyMsg)
	}
}

func (e *PubSubServer) handleSubscribe(topic string, cl *ServerClient) {
	log.Printf("PubSubServer: subscribe; topic=%s", topic)

	cl.subscriptionsByClient = append(cl.subscriptionsByClient, topic)

	e.clientBySubscription[topic] = append(e.clientBySubscription[topic], cl)
}

func (e *PubSubServer) readFromOneClient(cl *ServerClient, conn net.Conn) {
	log.Printf("PubSubServer: accepted connection from %s", conn.RemoteAddr())

	authenticated := false

	reader := bufio.NewReader(conn)

	// read until we want to disconnect
	for {
		rawMessage, errRead := reader.ReadString('\n')
		if errRead != nil {
			if errRead == io.EOF {
				log.Printf("PubSubServer: readFromOneClient: EOF encountered")
			} else {
				operr, ok := errRead.(*net.OpError)
				if ok && operr.Err.Error() == syscall.ECONNRESET.Error() {
					log.Printf("PubSubServer: readFromOneClient: error: Connection reset by beer")
				} else {
					log.Printf("PubSubServer: readFromOneClient: error: Type not ECONNRESET")
				}
			}

			break
		}

		// 'SET key value\n' => [ 'SET', 'key', 'value' ]
		msgParts := pubsub.MsgformatDecode(rawMessage)

		// FIXME: assert len(msgParts) > 0

		msgType := msgParts[0]

		if msgType == "PUB" && len(msgParts) == 3 {
			if !authenticated {
				log.Printf("PubSubServer: attempt to invoke privileged action while unauthorized")
				conn.Close()
				break
			}

			topic := msgParts[1]
			message := msgParts[2]

			e.handlePublish(topic, message, cl)
		} else if msgType == "SUB" && len(msgParts) == 2 {
			if !authenticated {
				log.Printf("PubSubServer: attempt to invoke privileged action while unauthorized")
				conn.Close()
				break
			}

			topic := msgParts[1]

			e.handleSubscribe(topic, cl)

			conn.Write([]byte(pubsub.MsgformatEncode([]string{"OK"})))
		} else if msgType == "AUTH" && len(msgParts) == 2 {
			if msgParts[1] == config.AUTH_TOKEN {
				authenticated = true
			}
		} else if msgType == "BYE" && len(msgParts) == 1 {
			conn.Close()
			break
		} else {
			panic(fmt.Errorf("Unsupported message type: %s", msgType))
		}
	}

	e.handleClientDisconnect(cl)
}

func (e *PubSubServer) acceptorLoop(listener net.Listener) {
	log.Printf("PubSubServer: starting acceptorLoop")

	for {
		conn, err := listener.Accept()
		if err != nil {
			break // TODO: panic if not EOF. Is it EOF when listener.Close()?
			// panic(err)
		}

		writeCh := make(chan string, 100)

		cl := ServerClient{
			Addr:                  conn.RemoteAddr().String(),
			writeCh:               writeCh, // FIXME: this is currently not used
			subscriptionsByClient: []string{},
			sendQueue:             partitionedlossyqueue.New(),
		}

		go e.writeForOneClient(&cl, conn)
		go e.readFromOneClient(&cl, conn)
	}

	e.acceptorDone <- true
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
