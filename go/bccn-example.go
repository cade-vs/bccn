package main

import (
	"fmt"
	"log"
	"os"

	"bccn/bccn"
)

func main() {
	conn, err := bccn.Open("255.255.255.255", 1122)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	conn.SetName(fmt.Sprintf("relay01/cardsys-relay/%d", os.Getpid()))

	if err := conn.Send("test",
		[]byte("txnid=12345|amount=1234|rc=00")); err != nil {
		log.Println("send:", err)
	}

	for {
		msg, err := conn.Recv()
		if err != nil {
			log.Println("recv:", err)
			continue
		}
		fmt.Printf("rx from=%s seq=%d chan=%s payload=%q peer=%s\n",
			msg.Src, msg.Seq, msg.Chan, msg.Payload, msg.Peer)
	}
}
