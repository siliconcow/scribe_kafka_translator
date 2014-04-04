package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/samuel/go-thrift/examples/scribe"
	"github.com/samuel/go-thrift/thrift"
	"log"
	"net"
	"net/rpc"
	"time"
)

var kafka_hostname string
var partition int
var port int
var buffer_size int
var buffer_time int

// implementation

type scribeServiceImplementation int

func (s *scribeServiceImplementation) Log(messages []*scribe.LogEntry) (scribe.ResultCode, error) {
	client, err := sarama.NewClient("client_id", []string{kafka_hostname}, &sarama.ClientConfig{MetadataRetries: 1, WaitForElection: 250 * time.Millisecond})
	if err != nil {
		log.Println(err)
	} else {
		log.Printf("Connected to %s\n", kafka_hostname)
	}
	defer client.Close()

	producer, err := sarama.NewProducer(client, &sarama.ProducerConfig{
		RequiredAcks: sarama.WaitForLocal,
		MaxBufferTime: uint32(buffer_time),
		MaxBufferedBytes: uint32((len(messages[0].Message) * buffer_size) - 1),
	})
	if err != nil {
		log.Println(err)
	}
	defer producer.Close()

	success := 0
	errors := 0
	startTime := time.Now()
	for _, m := range messages {
		log.Printf("Message Received: %+v\n", m)
		err = producer.SendMessage(m.Category, nil, sarama.StringEncoder(m.Message))
		if err != nil {
			errors++
			log.Println(err)
		} else {
			success++
		}
	}
	endTime := time.Now()
	log.Printf("Sent %d messages in %0.2d ms with %d errors", success, ((endTime.Sub(startTime))/1e6), errors)
	if errors > 0 {
		return scribe.ResultCodeTryLater, nil
	} else {
		return scribe.ResultCodeOk, nil
	}
}

func main() {
	scribeService := new(scribeServiceImplementation)
	rpc.RegisterName("Thrift", &scribe.ScribeServer{Implementation: scribeService})
	flag.IntVar(&port, "p", 1463, "Scribe Listen Port")
	flag.StringVar(&kafka_hostname, "o", "localhost:9092", "host:port string for the kafka server")
	flag.IntVar(&partition, "r", 1, "partition to publish to")
	flag.IntVar(&buffer_size, "s", 10, "Buffer Size")
	flag.IntVar(&buffer_time, "t", 10, "Buffer Time")

	flag.Parse()

	fmt.Printf("Quiet! I'm trying to listen to port %d and send to kafka at %s", port, kafka_hostname)
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Println(err)
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
