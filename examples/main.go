package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/gabrielperezs/streamspooler/firehose"
	"github.com/pquerna/ffjson/ffjson"
)

type record struct {
	TS int64  `json:"ts,omitempty"`
	S  string `json:"s,omitempty"`
}

func init() {
	rand.Seed(time.Now().UnixNano())
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
		StreamName: "firehoseStreamName",
		Profile:    "yourprofile",
		Region:     "eu-west-1",
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
			default:
				log.Printf("Channel closed or full")
				return
			}

			<-time.After(1 * time.Second)
		}
	}()

	go func() {
		<-time.After(60 * time.Second)
		p.Exit()
	}()

	p.Waiting()
}
