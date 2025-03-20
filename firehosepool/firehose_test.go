package firehosepool

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"testing"
	"time"

	// "github.com/aws/smithy-go/transport/http"

	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	"github.com/pquerna/ffjson/ffjson"
	"golang.org/x/exp/rand"
)

type record struct {
	TS int64  `json:"ts,omitempty"`
	S  string `json:"s,omitempty"`
}

func init() {
	rand.Seed(uint64(time.Now().UnixNano()))
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var numReq atomic.Int64

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestTrottlingError(t *testing.T) {

	c := Config{
		StreamName: "firehoseStreamName",
		Region:     "eu-west-1",
		// MaxRecords:     4,
		MinWorkers:     1,
		MaxWorkers:     1,
		Buffer:         1,
		FHClientGetter: &mockedClient{},
	}
	p, err := New(c)
	if err != nil {
		t.Fatalf("Firehose: %s\n", err)
	}

	r, _ := ffjson.Marshal(&record{
		TS: int64(time.Now().UnixNano() / int64(time.Millisecond)),
		S:  "testing msg",
	})

	p.C <- r
	log.Printf("test message sent")

	for trials := 0; len(p.clients) == 0 && trials < 5; trials++ {
		time.Sleep(1 * time.Second)
	}
	if len(p.clients) == 0 {
		t.Fatalf("Firehose: no client created\n")
	}

	err = p.Flush()
	if err == nil {
		t.Fatal("No Thottle error received")
	}

	if numReq.Load() == 0 {
		t.Fatalf("Firehose: no AWS Requests made\n")
	}

	if p.errors == 0 {
		// should do some errors and retries, due to forced throttling error
		t.Fatalf("Firehose: errors %d\n", p.errors)
	}

	<-time.After(100 * time.Millisecond)

	p.Exit()

	p.Waiting()

	if p.errors < numReq.Load()-1 {
		t.Fatalf("Firehose: errors %d < requests -1  (%d)\n", p.errors, numReq.Load()-1)
	}
	fmt.Printf("Firehose mocked requests received: %d\n", numReq.Load())
	fmt.Printf("Firehose srv forced errors count: %d\n", p.errors)
}

// func TestPutRecordBatchThrottling(t *testing.T) {

// 	c := Config{
// 		StreamName: "firehoseStreamName",
// 		Region:     "eu-west-1",
// 		// MaxRecords:     4,
// 		MinWorkers:     1,
// 		MaxWorkers:     1,
// 		FHClientGetter: &mockedClient{},
// 	}
// 	p, err := New(c)
// 	if err != nil {
// 		t.Fatalf("Firehose: %s\n", err)
// 	}

// 	go func() {
// 		for i := 0; i < 10; i++ {
// 			r, err := ffjson.Marshal(&record{
// 				TS: int64(time.Now().UnixNano() / int64(time.Millisecond)),
// 				S:  fmt.Sprintf("%s: %s", "testing bytes: Testing", RandStringRunes(10)),
// 			})

// 			if err != nil {
// 				log.Panicf("E: %s", err)
// 			}

// 			select {
// 			case p.C <- r:
// 				fmt.Println("sent ", i)
// 			default:
// 				log.Printf("Channel closed or full")
// 			}
// 		}
// 	}()

// 	go func() {
// 		<-time.After(1 * time.Second)
// 		p.Flush()
// 	}()

// 	go func() {
// 		<-time.After(2 * time.Second)
// 		p.Exit()
// 	}()

// 	p.Waiting()

// 	if numReq.Load() == 0 {
// 		t.Fatalf("Firehose: no AWS Requests made\n")
// 	}

// 	if p.errors == 0 {
// 		// should do some errors and retries, due to forced throttling error
// 		t.Fatalf("Firehose: errors %d\n", p.errors)
// 	}

// 	if p.errors < numReq.Load()-1 {
// 		t.Fatalf("Firehose: errors %d < requests -1  (%d)\n", p.errors, numReq.Load()-1)
// 	}
// 	fmt.Printf("Firehose mocked requests received: %d\n", numReq.Load())
// 	fmt.Printf("Firehose srv forced errors count: %d\n", p.errors)
// 	fmt.Printf("Firehose messages lost (normal due to exit): %d\n", len(p.C))
// }

type mockedClient struct{}

func (m *mockedClient) GetClient(cfg *Config) (*firehose.Client, error) {
	mw := middleware.FinalizeMiddlewareFunc("testMw", func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
		out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
	) {
		numReq.Add(1)
		fmt.Println("MOCK firehose request")
		// Simulate Throttling error
		return middleware.FinalizeOutput{}, middleware.Metadata{}, &smithy.GenericAPIError{
			Code:    "ThrottlingException",
			Message: "Request throttled due to rate limiting",
			Fault:   smithy.FaultClient,
		}
	})

	// Create client with mock handler
	cli := firehose.New(firehose.Options{
		APIOptions: []func(*middleware.Stack) error{
			func(s *middleware.Stack) error {
				s.Finalize.Clear()
				s.Initialize.Clear()
				return s.Finalize.Add(mw, middleware.After)
			},
		},
	})
	return cli, nil
}
