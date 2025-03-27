package main

import (
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"net/http"
	"time"

	"github.com/gabrielperezs/streamspooler/v2/firehosepool"
	"github.com/pquerna/ffjson/ffjson"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	// logger := slog.New(slog.NewTextHandler(os.Stderr,
	// 	&slog.HandlerOptions{Level: slog.LevelDebug}))
	// slog.SetDefault(logger)

	c := firehosepool.Config{
		StreamName: "testing-stream",
		// Profile:        "yourprofile",
		Region:         "eu-west-1",
		MaxConcatLines: 25,
		Buffer:         5,
		ConcatRecords:  true,
		MinWorkers:     1,
		MaxWorkers:     5,
		LogBatchAppend: true,
		LogRecordWrite: true,
		EnableMetrics:  true,
	}
	p, err := firehosepool.New(c)
	if err != nil {
		log.Fatalf("Error starting firehose: %s\n", err)
	}

	go serveMetrics()

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
				slog.Info("Record sent")
			default:
				slog.Error("Channel full")
				return
			}

			<-time.After(500 * time.Millisecond)
		}
	}()

	go func() {
		<-time.After(160 * time.Second)
		p.Exit()
	}()

	p.Waiting()
}

func serveMetrics() {
	prometheus.Unregister(collectors.NewGoCollector())
	prometheus.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	http.Handle("/metrics", promhttp.Handler())
	slog.Error("metrics and debug server", "error", http.ListenAndServe(":6060", nil))
}
