package server

import (
	"bufio"
	"fmt"
	"github.com/function61/pyramid/pubsub"
	"io"
	"log"
	"net"
	"syscall"
)

/*	Performance

	Initial:

		450 000 msgs/sec

	Write channel buffer increase 10 -> 100:

		519 000 msgs/sec

	Server write batching, batchSize=4k

		744 000 msgs/sec

	Server write batching, batchSize=8k

		723 000 msgs/sec

	Server write batching, batchSize=2k

		763 000 msgs/sec

	Writes go now through channels

		503 326 msgs/sec


	Observations:

	- No improvement when getting rid of msgformatEncode()

	Methodology:

	- Within a single container: Producer -> Server -> Consumer

*/

// client from the server's perspective
type ServerClient struct {
	Addr                  string
	writeCh               chan string
	subscriptionsByClient []string
}

type ClientsBySubscription map[string][]*ServerClient

type ESPubSubServer struct {
	clientBySubscription ClientsBySubscription
	listener             net.Listener
	acceptorDone         chan bool
}

func NewESPubSubServer(bindAddr string) *ESPubSubServer {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		panic(err)
	}

	log.Printf("ESPubSubServer: binding to %s", bindAddr)

	e := &ESPubSubServer{
		clientBySubscription: make(ClientsBySubscription),
		listener:             listener,
		acceptorDone:         make(chan bool),
	}

	go e.acceptorLoop(listener)

	return e
}

func (e *ESPubSubServer) Close() {
	log.Printf("ESPubSubServer: closing. Stopping acceptorLoop.")

	e.listener.Close()

	<-e.acceptorDone

	log.Printf("ESPubSubServer: acceptor shut down")
}

func (e *ESPubSubServer) handleClientDisconnect(cl *ServerClient) {
	e.removeClientSubscriptions(cl)
}

func (e *ESPubSubServer) writeForOneClient(cl *ServerClient, conn net.Conn) {
	/*
		Read until does not block (or buffer full)

		Then do a conn.Write()
	*/
	for {
		msgToWrite := <-cl.writeCh

		// see if we have more messages, so we can batch the conn.Write()
		continuePeeking := true

		for continuePeeking {

			select {
			case additionalMsg := <-cl.writeCh:
				msgToWrite = msgToWrite + additionalMsg

				if len(msgToWrite) > 2*1024 {
					// log.Printf("Batched already 4 kB")
					// break
					continuePeeking = false
				}
			default:
				continuePeeking = false
				// break // no activity
			}
		}

		if _, err := conn.Write([]byte(msgToWrite)); err != nil {
			log.Printf("ESPubSubServer: write error to client %s. Stopping writer.", cl.Addr)
			e.handleClientDisconnect(cl)
			// panic(err)
			break
		}
	}
}

func (e *ESPubSubServer) handlePublish(topic string, message string, cl *ServerClient) {
	// log.Printf("Publish; topic=%s message=%s", topic, message)

	for _, subscriberClient := range e.clientBySubscription[topic] {
		notifyMsg := pubsub.MsgformatEncode([]string{"NOTIFY", topic, message})

		subscriberClient.writeCh <- notifyMsg

		// write notify message on channel, but only if it does not block
		// http://stackoverflow.com/questions/25657207/golang-how-to-know-a-buffered-channel-is-full
		/*
			select {
			case subscriberClient.writeCh <- notifyMsg:
			default:
				fmt.Println("Channel full. Discarding value")
			}
		*/
	}
}

func (e *ESPubSubServer) handleSubscribe(topic string, cl *ServerClient) {
	log.Printf("ESPubSubServer: subscribe; topic=%s", topic)

	cl.subscriptionsByClient = append(cl.subscriptionsByClient, topic)

	e.clientBySubscription[topic] = append(e.clientBySubscription[topic], cl)
}

func (e *ESPubSubServer) readFromOneClient(cl *ServerClient, conn net.Conn) {
	log.Printf("ESPubSubServer: accepted connection from %s", conn.RemoteAddr())

	reader := bufio.NewReader(conn)

	// read until we want to disconnect
	for {
		rawMessage, errRead := reader.ReadString('\n')
		if errRead != nil {
			if errRead == io.EOF {
				log.Printf("ESPubSubServer: readFromOneClient: EOF encountered")
			} else {
				operr, ok := errRead.(*net.OpError)
				if ok && operr.Err.Error() == syscall.ECONNRESET.Error() {
					log.Printf("ESPubSubServer: readFromOneClient: error: Connection reset by beer")
				} else {
					log.Printf("ESPubSubServer: readFromOneClient: error: Type not ECONNRESET")
				}
			}

			break
		}

		// 'SET key value\n' => [ 'SET', 'key', 'value' ]
		msgParts := pubsub.MsgformatDecode(rawMessage)

		msgType := msgParts[0]

		if msgType == "PUB" {
			topic := msgParts[1]
			message := msgParts[2]

			e.handlePublish(topic, message, cl)
		} else if msgType == "SUB" {
			topic := msgParts[1]

			e.handleSubscribe(topic, cl)

			conn.Write([]byte(pubsub.MsgformatEncode([]string{"OK"})))
		} else if msgType == "BYE" {
			conn.Close()
			break
		} else {
			panic(fmt.Errorf("Unsupported message type: %s", msgType))
		}
	}

	e.handleClientDisconnect(cl)
}

func (e *ESPubSubServer) acceptorLoop(listener net.Listener) {
	log.Printf("ESPubSubServer: starting acceptorLoop")

	for {
		conn, err := listener.Accept()
		if err != nil {
			break // TODO: panic if not EOF. Is it EOF when listener.Close()?
			// panic(err)
		}

		writeCh := make(chan string, 100)

		cl := ServerClient{
			conn.RemoteAddr().String(),
			writeCh,
			[]string{},
		}

		go e.writeForOneClient(&cl, conn)
		go e.readFromOneClient(&cl, conn)
	}

	e.acceptorDone <- true
}

func (e *ESPubSubServer) removeClientSubscriptions(cl *ServerClient) {
	for _, topic := range cl.subscriptionsByClient {

		foundIndex := -1
		for idx, value := range e.clientBySubscription[topic] { // find position of element
			if value == cl {
				foundIndex = idx
				break
			}
		}

		if foundIndex != -1 { // delete by position
			log.Printf("ESPubSubServer: removeClientSubscriptions: removing subscription. topic=%s pos=%d", topic, foundIndex)

			temp := e.clientBySubscription[topic]

			// log.Printf("Before %v", temp)

			temp = append(
				temp[:foundIndex],
				temp[foundIndex+1:]...)

			e.clientBySubscription[topic] = temp

			// log.Printf("After %v", temp)
		} else {
			log.Printf("ESPubSubServer: removeClientSubscriptions: sub not found. SHOULD NOT HAPPEN. topic=%s", topic)
		}
	}
}
