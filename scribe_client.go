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

func main() {
	flag.IntVar(&port, "p", 1463, "Scribe Port")
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
	res, err := scr.Log([]*scribe.LogEntry{{category,string(message)}})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Response: %+v\n", res)
}
