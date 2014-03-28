package main

import (
	"fmt"
	"net"
	"net/rpc"
	"flag"
	kafka "github.com/jdamick/kafka"
	"github.com/samuel/go-thrift/examples/scribe"
	"github.com/samuel/go-thrift/thrift"
)

var kafka_hostname string
var partition int
var port int

// implementation

type scribeServiceImplementation int

func (s *scribeServiceImplementation) Log(messages []*scribe.LogEntry) (scribe.ResultCode, error) {
        broker := kafka.NewBrokerPublisher(kafka_hostname, messages[0].Category, partition) //standard scribe splits category by thread/connection
	timing := kafka.StartTiming("Forwarding messages...")
	for _, m := range messages {
		fmt.Printf("MSG: %+v\n", m)
		bytes := []byte(m.Message)
		broker.Publish(kafka.NewMessage(bytes))
	}
	timing.Print()
	return scribe.ResultCodeOk, nil
}

func main() {
	scribeService := new(scribeServiceImplementation)
	rpc.RegisterName("Thrift", &scribe.ScribeServer{Implementation: scribeService})
	flag.IntVar(&port, "port", 1463, "Scribe Listen Port")
	flag.StringVar(&kafka_hostname, "kafka_hostname", "localhost:9092", "host:port string for the kafka server")
	flag.IntVar(&partition, "partition", 0, "partition to publish to")

	flag.Parse()

	fmt.Printf("Quiet! I'm lisening on port %d and sending to kafka host at %s", port, kafka_hostname)
	ln, err := net.Listen("tcp", ":1463" )
	if err != nil {
		panic(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("ERROR: %+v\n", err)
			continue
		}
		fmt.Printf("New connection %+v\n", conn)
		go rpc.ServeCodec(thrift.NewServerCodec(thrift.NewFramedReadWriteCloser(conn, 0), thrift.NewBinaryProtocol(true, false)))
	}
}
