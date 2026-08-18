package firehosepool

import (
	"context"
	"fmt"
	"log"
	"sync"
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
	// numReq is package level (the mocked client has no other way to report),
	// so reset it to keep runs with -count>1 independent.
	numReq.Store(0)

	c := Config{
		StreamName: "firehoseStreamName",
		Region:     "eu-west-1",
		// MaxRecords:     4,
		MinWorkers:     1,
		MaxWorkers:     1,
		Buffer:         1,
		FHClientGetter: &mockedClient{},
		LogBatchAppend: true,
		LogRecordWrite: true,
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

	for trials := 0; len(p.clientsSnapshot()) == 0 && trials < 5; trials++ {
		time.Sleep(50 * time.Millisecond)
	}
	if len(p.clientsSnapshot()) == 0 {
		t.Fatalf("Firehose: no client created\n")
	}

	// The worker may not have consumed the record from p.C yet, and a flush with
	// an empty batch is a no-op that returns no error. Retry until it has
	// something to send.
	for range 20 {
		if err = p.Flush(); err != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err == nil {
		t.Fatal("No Thottle error received")
	}

	if numReq.Load() == 0 {
		t.Fatalf("Firehose: no AWS Requests made\n")
	}

	pErrs := p.Errors()
	if pErrs == 0 {
		// should do some errors and retries, due to forced throttling error
		t.Fatalf("Firehose: errors %d\n", pErrs)
	}

	<-time.After(100 * time.Millisecond)

	p.Exit()

	p.Waiting()

	pErrs = p.Errors()
	if pErrs < numReq.Load()-1 {
		t.Fatalf("Firehose: errors %d < requests -1  (%d)\n", pErrs, numReq.Load()-1)
	}
	fmt.Printf("Firehose mocked requests received: %d\n", numReq.Load())
	fmt.Printf("Firehose srv forced errors count: %d\n", pErrs)
}

type mockedClient struct{}

func (m *mockedClient) GetClient(cfg *Config) (*firehose.Client, error) {
	mw := middleware.FinalizeMiddlewareFunc("testMw", func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
		out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
	) {
		numReq.Add(1)
		log.Println("test mocked firehose request received")
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

// A stream retired right after it was built used to panic: the scaling
// goroutine could add a worker after Exit had snapshotted the pool, and that
// worker then read from the channel Exit closed, turning the nil the closed
// channel hands out into "interface conversion: interface {} is nil".
func TestExitWhileWorkersAreStarting(t *testing.T) {
	for range 200 {
		p, err := New(Config{
			StreamName:     "firehoseStreamName",
			Region:         "eu-west-1",
			MinWorkers:     1,
			MaxWorkers:     2,
			Buffer:         1,
			FHClientGetter: &quietClient{},
		})
		if err != nil {
			t.Fatalf("Firehose: %s\n", err)
		}

		// Exit ends with a blocking send that only Waiting receives, so the two
		// have to run on different goroutines. This is how a consumer retires a
		// stream, see proxymd's internal/logs.exit.
		go p.Exit()
		p.Waiting()
	}
}

// Scaling down hands a worker to its own Exit goroutine and drops it from
// srv.clients, so a Server.Exit running at the same time does not see it in its
// snapshot. If that worker is busy flushing when Exit closes the channel, it
// comes back to a select where the closed channel is ready, which used to panic
// on the nil the receive hands out.
func TestExitWhileWorkerIsDetached(t *testing.T) {
	for range 100 {
		p, err := New(Config{
			StreamName:     "firehoseStreamName",
			Region:         "eu-west-1",
			MinWorkers:     1,
			MaxWorkers:     2,
			Buffer:         128,
			FlushTimeout:   time.Millisecond,
			FHClientGetter: &quietClient{},
		})
		if err != nil {
			t.Fatalf("Firehose: %s\n", err)
		}
		for trials := 0; len(p.clientsSnapshot()) == 0 && trials < 200; trials++ {
			time.Sleep(time.Millisecond)
		}

		// Give the worker something to send: the mocked client throttles, so it
		// stays inside the flush while Exit closes the channel underneath it.
		for range 20 {
			select {
			case p.C <- []byte("some record to flush"):
			default:
			}
		}
		time.Sleep(2 * time.Millisecond)

		p.cliDesired.Store(0)
		p.clientsReset() // detaches the busy worker and exits it in a goroutine

		go p.Exit()
		p.Waiting()
	}
}

// quietClient is mockedClient without the package-level request counter, so the
// teardown tests do not inflate what TestTrottlingError asserts on: their
// workers can still be sending when the next test starts.
type quietClient struct{}

func (m *quietClient) GetClient(cfg *Config) (*firehose.Client, error) {
	mw := middleware.FinalizeMiddlewareFunc("quietMw", func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
		out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
	) {
		return middleware.FinalizeOutput{}, middleware.Metadata{}, &smithy.GenericAPIError{
			Code:    "ThrottlingException",
			Message: "Request throttled due to rate limiting",
			Fault:   smithy.FaultClient,
		}
	})

	return firehose.New(firehose.Options{
		APIOptions: []func(*middleware.Stack) error{
			func(s *middleware.Stack) error {
				s.Finalize.Clear()
				s.Initialize.Clear()
				return s.Finalize.Add(mw, middleware.After)
			},
		},
	}), nil
}

// Flush failures signal a reload on chReload, the channel Exit closes. A stream
// retired while its records are failing used to panic with "send on closed
// channel"; the signal now goes out under the lock that Exit sets exiting with.
func TestFailureDuringExit(t *testing.T) {
	for range 200 {
		p, err := New(Config{
			StreamName:     "firehoseStreamName",
			Region:         "eu-west-1",
			MinWorkers:     1,
			MaxWorkers:     1,
			Buffer:         1,
			FHClientGetter: &quietClient{},
		})
		if err != nil {
			t.Fatalf("Firehose: %s\n", err)
		}

		// More than maxErrors per goroutine, so the reload signal is actually
		// sent and not swallowed by the error threshold.
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 50 {
					p.failure()
				}
			}()
		}

		go p.Exit()
		p.Waiting()
		wg.Wait()
	}
}
