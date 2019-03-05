package pubsubPool

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/gallir/bytebufferpool"
	compress "github.com/gallir/smart-relayer/redis"
)

const (
	recordsTimeout  = 15 * time.Second
	maxRecordSize   = 1000 * 1000
	maxBatchRecords = 100
	maxBatchSize    = 3 << 20 // 4 MiB per call

	partialFailureWait = 200 * time.Millisecond
	globalFailureWait  = 500 * time.Millisecond
	onFlyRetryLimit    = 1024 * 2
	firehoseError      = "InternalFailure"
)

var (
	clientCount int64
	newLine     = []byte("\n")
)

var pool = &bytebufferpool.Pool{}

// Client is the thread that connect to the remote redis server
type Client struct {
	sync.Mutex
	srv         *Server
	mode        int
	buff        *bytebufferpool.ByteBuffer
	count       int
	batch       []*bytebufferpool.ByteBuffer
	batchSize   int
	done        chan bool
	finish      chan bool
	ID          int64
	t           *time.Timer
	lastFlushed time.Time
	onFlyRetry  int64
}

// NewClient creates a new client that connects to a Firehose
func NewClient(srv *Server) *Client {
	n := atomic.AddInt64(&clientCount, 1)

	clt := &Client{
		done:   make(chan bool),
		finish: make(chan bool),
		srv:    srv,
		ID:     n,
		t:      time.NewTimer(recordsTimeout),
		batch:  make([]*bytebufferpool.ByteBuffer, 0, maxBatchRecords),
		buff:   pool.Get(),
	}

	go clt.listen()

	return clt
}

func (clt *Client) listen() {
	log.Printf("PubSub client %s [%d]: ready", clt.srv.cfg.Project, clt.ID)
	for {

		select {
		case ri := <-clt.srv.C:
			var r []byte
			if clt.srv.cfg.Serializer != nil {
				var err error
				if r, err = clt.srv.cfg.Serializer(ri); err != nil {
					log.Printf("PubSub client %s [%d]: ERROR serializer: %s", clt.srv.cfg.Project, clt.ID, err)
					continue
				}
			} else {
				r = ri.([]byte)
			}

			recordSize := len(r)

			if recordSize <= 0 {
				continue
			}

			if clt.srv.cfg.Compress {
				// All the message will be compress. This will work with raw and json messages.
				r = compress.Bytes(r)
				// Update the record size using the compression []byte result
				recordSize = len(r)
			}

			if recordSize > maxRecordSize {
				log.Printf("PubSub client %s [%d]: ERROR: one record is over the limit %d/%d", clt.srv.cfg.Project, clt.ID, recordSize, maxRecordSize)
				continue
			}

			clt.count++

			if clt.count >= clt.srv.cfg.MaxRecords || len(clt.batch) >= maxBatchRecords {
				clt.flush()
			}

			// The maximum size of a record sent to Kinesis Firehose, before base64-encoding, is 1000 KB.
			if !clt.srv.cfg.ConcatRecords || clt.buff.Len()+recordSize+1 >= maxRecordSize || clt.count >= clt.srv.cfg.MaxRecords {
				if clt.buff.Len() > 0 {
					// Save in new record
					buff := clt.buff
					clt.batch = append(clt.batch, buff)
					clt.buff = pool.Get()
				}
			}

			clt.buff.Write(r)
			clt.buff.Write(newLine)

			clt.batchSize += clt.buff.Len()

			if len(clt.batch)+1 >= maxBatchRecords {
				clt.flush()
			}

		case <-clt.t.C:
			clt.flush()
			if clt.buff.Len() > 0 {
				buff := clt.buff
				clt.batch = append(clt.batch, buff)
				clt.buff = pool.Get()
				clt.flush()
			}
		case <-clt.finish:
			//Stop and drain the timer channel
			if !clt.t.Stop() {
				select {
				case <-clt.t.C:
				default:
				}
			}

			clt.flush()
			if clt.buff.Len() > 0 {
				clt.batch = append(clt.batch, clt.buff)
				clt.flush()
			}

			if l := len(clt.batch); l > 0 {
				log.Printf("PubSub client %s [%d]: Exit, %d records lost", clt.srv.cfg.Project, clt.ID, l)
				return
			}

			log.Printf("PubSub client %s [%d]: Exit", clt.srv.cfg.Project, clt.ID)
			clt.done <- true
			return
		}
	}
}

// flush build the last record if need and send the records slice to AWS Firehose
func (clt *Client) flush() {

	if !clt.t.Stop() {
		select {
		case <-clt.t.C:
		default:
		}
	}
	clt.t.Reset(recordsTimeout)

	size := len(clt.batch)
	// Don't send empty batch
	if size == 0 {
		return
	}

	// Add context timeout to the request
	ctx := context.Background()
	for _, b := range clt.batch {
		result := clt.srv.topicCli.Publish(ctx, &pubsub.Message{
			Data: b.B,
		})
		// Block until the result is returned and a server-generated
		// ID is returned for the published message.
		_, err := result.Get(ctx)
		if err != nil {
			log.Printf("Error: %s", err)
			return
		}
		//log.Printf("PubSub client %s [%d]: Published a message; msg ID: %v", clt.srv.cfg.Project, clt.ID, id)
	}

	// Put slice bytes in the pull after sent
	for _, b := range clt.batch {
		pool.Put(b)
	}

	clt.batchSize = 0
	clt.count = 0
	clt.batch = nil
}

// Exit finish the go routine of the client
func (clt *Client) Exit() {
	clt.finish <- true
	<-clt.done
}

func (clt *Client) retry(orig []byte) {
	// Remove the last byte, is a newLine
	b := make([]byte, len(orig)-len(newLine))
	copy(b, orig[:len(orig)-len(newLine)])

	go func(b []byte) {
		atomic.AddInt64(&clt.onFlyRetry, 1)
		defer atomic.AddInt64(&clt.onFlyRetry, -1)
		clt.srv.C <- b
	}(b)
}
