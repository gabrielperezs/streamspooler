package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/gabrielperezs/streamspooler/v2/firehosepool"
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
	c := firehosepool.Config{
		StreamName:     "webbeds-testing-stream",
		Profile:        "yourprofile",
		Region:         "eu-west-1",
		MaxConcatLines: 25,
		Buffer:         5,
		ConcatRecords:  true,
		// TODO check final values. added this
		MinWorkers: 1,
		MaxWorkers: 5,
	}
	p, err := firehosepool.New(c)
	if err != nil {
		log.Fatalf("Error starting firehose: %s\n", err)
	}
	// TODO remove this sleep
	time.Sleep(2 * time.Second)

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
				log.Printf("Channel full")
				return
			}

			// TODO it was 400ms
			<-time.After(1 * time.Microsecond)
		}
	}()

	go func() {
		<-time.After(160 * time.Second)
		p.Exit()
	}()

	p.Waiting()
}
