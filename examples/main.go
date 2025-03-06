package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	firehosePool "github.com/gabrielperezs/streamspooler/firehose"
	"github.com/pquerna/ffjson/ffjson"
)

type record struct {
	TS int64  `json:"ts,omitempty"`
	S  string `json:"s,omitempty"`
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func main() {
	c := firehosePool.Config{
		StreamName:    "webbeds-testing-stream",
		Profile:       "yourprofile",
		Region:        "eu-west-1",
		MaxRecords:    25,
		Buffer:        5,
		ConcatRecords: true,
	}
	p := firehosePool.New(c)

	go func() {

		for {
			r, err := ffjson.Marshal(&record{
				TS: int64(time.Now().UnixNano() / int64(time.Millisecond)),
				S:  fmt.Sprintf("%s: %s", "testing bytes: España", RandStringRunes(10)),
			})

			if err != nil {
				log.Panicf("E: %s", err)
			}

			select {
			case p.C <- r:
				fmt.Printf("Record sent\n")
			default:
				log.Printf("Channel closed or full")
				return
			}

			<-time.After(400 * time.Millisecond)
		}
	}()

	go func() {
		<-time.After(160 * time.Second)
		p.Exit()
	}()

	p.Waiting()
}
