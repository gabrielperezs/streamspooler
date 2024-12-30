package firehosePool

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

func TestPutRecordBatchThrottling(t *testing.T) {

	c := Config{
		StreamName: "firehoseStreamName",
		Region:     "eu-west-1",
		MaxRecords: 4,
	}
	p := NewTestFirehoseServer(c)

	go func() {
		for i := 0; i < 10; i++ {
			r, err := ffjson.Marshal(&record{
				TS: int64(time.Now().UnixNano() / int64(time.Millisecond)),
				S:  fmt.Sprintf("%s: %s", "testing bytes: Testing", RandStringRunes(10)),
			})

			if err != nil {
				log.Panicf("E: %s", err)
			}

			select {
			case p.C <- r:
				fmt.Println("sent ", i)
			default:
				log.Printf("Channel closed or full")
			}
		}
	}()

	go func() {
		<-time.After(2 * time.Second)
		p.Exit()
	}()

	p.Waiting()

	if numReq.Load() == 0 {
		t.Fatalf("Firehose: no AWS Requests made\n")
	}

	if p.errors == 0 {
		// should do some errors and retries, due to forced throttling error
		t.Fatalf("Firehose: errors %d\n", p.errors)
	}

	if p.errors < numReq.Load()-1 {
		t.Fatalf("Firehose: errors %d < requests -1  (%d)\n", p.errors, numReq.Load()-1)
	}
	fmt.Printf("Firehose mocked requests received: %d\n", numReq.Load())
	fmt.Printf("Firehose srv forced errors count: %d\n", p.errors)
	fmt.Printf("Firehose messages lost (normal due to exit): %d\n", len(p.C))
}

// New implemented to overwrite the aws-sdk-go-v2 client, to mock the requests.
// done this way to avoid changing firehose.New(), but would be better to implement a way to inject the client.
func NewTestFirehoseServer(cfg Config) *Server {
	if cfg.Buffer == 0 {
		cfg.Buffer = defaultBufferSize
	}

	srv := &Server{
		chDone:   make(chan bool, 1),
		chReload: make(chan bool, 1),
		C:        make(chan interface{}, cfg.Buffer),
		Fhcg:     &mockedClient{},
	}

	go srv._reload()
	srv.Reload(&cfg)

	return srv
}

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
