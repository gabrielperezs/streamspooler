package firehosePool

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/gallir/bytebufferpool"
	"github.com/gallir/smart-relayer/redis"
)

const (
	recordsTimeout  = 15 * time.Second
	maxRecordSize   = 1000 * 1024     // The maximum size of a record sent to Kinesis Firehose, before base64-encoding, is 1000 KB
	maxBatchRecords = 500             // The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller. This limit cannot be changed.
	maxBatchSize    = 4 * 1024 * 1024 // 4 MB per call
)

var (
	clientCount int64 = 0
	newLine           = []byte("\n")
)

var pool = &bytebufferpool.Pool{}

var recordPool = sync.Pool{
	New: func() interface{} {
		return &firehose.Record{}
	},
}

func recordPoolGet() *firehose.Record {
	return recordPool.Get().(*firehose.Record)
}

func recordPoolPut(r *firehose.Record) {
	r.Data = r.Data[:0]
	recordPool.Put(r)
}

// Client is the thread that connect to the remote redis server
type Client struct {
	sync.Mutex
	srv         *Server
	mode        int
	buff        *bytebufferpool.ByteBuffer
	count       int
	batch       []*bytebufferpool.ByteBuffer
	batchSize   int
	status      int
	finish      chan bool
	done        chan bool
	ID          int64
	t           *time.Timer
	lastFlushed time.Time
}

// NewClient creates a new client that connect to a Redis server
func NewClient(srv *Server) *Client {
	n := atomic.AddInt64(&clientCount, 1)

	clt := &Client{
		done:   make(chan bool),
		finish: make(chan bool),
		status: 0,
		srv:    srv,
		ID:     n,
		t:      time.NewTimer(recordsTimeout),
	}

	if err := clt.Reload(); err != nil {
		log.Printf("Firehose client %d ERROR: reload %s", clt.ID, err)
		return nil
	}

	//log.Printf("Firehose client %d ready", clt.ID)

	return clt
}

// Reload finish the listener and run it again
func (clt *Client) Reload() error {
	clt.Lock()
	defer clt.Unlock()

	if clt.status > 0 {
		clt.Exit()
	}

	go clt.listen()

	return nil
}

func (clt *Client) listen() {
	defer log.Printf("Firehose client %d: Closed listener", clt.ID)

	clt.status = 1

	clt.buff = pool.Get()

	for {

		select {
		case r := <-clt.srv.C:

			recordSize := len(r)

			if recordSize <= 0 {
				continue
			}

			clt.count++

			if clt.srv.cfg.Compress {
				// All the message will be compress. This will work with raw and json messages.
				r = compress.Bytes(r)
				// Update the record size using the compression []byte result
				recordSize = len(r)
			}

			if recordSize+1 >= maxBatchSize {
				log.Printf("Firehose client %d: EFRROR: one record is over the limit %d/%d", clt.ID, recordSize, maxBatchSize)
				continue
			}

			// The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller. This limit cannot be changed.
			if clt.count >= clt.srv.cfg.MaxRecords || len(clt.batch)+1 > maxBatchRecords || clt.batchSize+recordSize+1 >= maxBatchSize {
				//log.Printf("flush: count %d/%d | batch %d/%d | size [%d] %d/%d",
				//	clt.count, clt.srv.cfg.MaxRecords, len(clt.batch), maxBatchRecords, recordSize, (clt.batchSize+recordSize+1)/1024, maxBatchSize/1024)
				// Force flush
				clt.flush()
			}

			// The maximum size of a record sent to Kinesis Firehose, before base64-encoding, is 1000 KB.
			if !clt.srv.cfg.ConcatRecords || clt.buff.Len()+recordSize+1 >= maxRecordSize || clt.count+1 > clt.srv.cfg.MaxRecords {
				if clt.buff.Len() > 0 {
					// Save in new record
					clt.batch = append(clt.batch, clt.buff)
					clt.buff = pool.Get()
				}
			}

			clt.buff.Write(r)
			clt.buff.Write(newLine)

			clt.batchSize += clt.buff.Len()

		case <-clt.t.C:
			if clt.buff.Len() > 0 {
				clt.batch = append(clt.batch, clt.buff)
				clt.buff = pool.Get()
			}

			clt.flush()

		case <-clt.done:
			log.Printf("Firehose client %d: closing..", clt.ID)
			clt.finish <- true
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

	// Don't send empty batch
	if len(clt.batch) == 0 {
		return
	}

	var batchRecords []*firehose.Record

	// Put slice in the pull after sent to AWS Firehose
	for _, r := range clt.batch {
		batchRecords = append(batchRecords, recordPoolGet().SetData(r.Bytes()))
		// Put slice buffers in the pull after sent
		pool.Put(r)
	}

	// Send the batch to AWS Firehose
	clt.putRecordBatch(batchRecords)

	// Put slice records in the pull after sent
	for _, r := range batchRecords {
		recordPoolPut(r)
	}

	clt.batchSize = 0
	clt.batch = nil
	clt.count = 0
}

// putRecordBatch is the client connection to AWS Firehose
func (clt *Client) putRecordBatch(records []*firehose.Record) {
	req, _ := clt.srv.awsSvc.PutRecordBatchRequest(&firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(clt.srv.cfg.StreamName),
		Records:            records,
	})

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	req.SetContext(ctx)

	err := req.Send()
	if err != nil {
		if req.IsErrorThrottle() {
			log.Printf("Firehose client %d: ERROR IsErrorThrottle: %s", clt.ID, err)
		} else {
			log.Printf("Firehose client %d: ERROR PutRecordBatch->Send: %s", clt.ID, err)
		}
		clt.srv.failure()
		return
	}

	//log.Printf("Firehose client %d: sent batch with %d records, %d lines, %d bytes", clt.ID, len(records), clt.count, clt.batchSize)
}

// Exit finish the go routine of the client
func (clt *Client) Exit() {
	defer log.Printf("Firehose client %d: Exit, %d records lost", clt.ID, len(clt.batch))

	if !clt.t.Stop() {
		select {
		case <-clt.t.C:
		default:
		}
	}

	clt.done <- true
	<-clt.finish

	if clt.buff.Len() > 0 {
		clt.batch = append(clt.batch, clt.buff)
	}

	clt.flush()
}
