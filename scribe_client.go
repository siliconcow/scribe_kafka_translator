package main

import (
	"fmt"
	"net"
  "os"
  "flag"
  "io/ioutil"

	"github.com/samuel/go-thrift/examples/scribe"
	"github.com/samuel/go-thrift/thrift"
)

var port int
var category string
var host string
var count int

func main() {
	flag.IntVar(&port, "p", 1463, "Scribe Port")
	flag.IntVar(&count, "c", 100, "Test messages")
	flag.StringVar(&host, "h", "localhost", "Scribe Host")
	flag.StringVar(&category, "c", "category", "Scribe Category")
  flag.Parse()
  message, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		panic(err)
	}
  fmt.Println(os.Args)
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:1463", host))
	if err != nil {
		panic(err)
	}

	client := thrift.NewClient(thrift.NewFramedReadWriteCloser(conn, 0), thrift.NewBinaryProtocol(true, false), false)
	scr := scribe.ScribeClient{Client: client}
  arr := make([]*scribe.LogEntry,count)
  for i :=0; i < count; i++{
   arr[i]=&scribe.LogEntry{category,string(message)}
  }
	res, err := scr.Log(arr)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Response: %+v\n", res)
}

