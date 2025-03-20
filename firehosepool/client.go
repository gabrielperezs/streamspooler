package firehosepool

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/smithy-go"
	"github.com/gabrielperezs/streamspooler/v2/internal/compress"
	"github.com/gallir/bytebufferpool"
)

const (
	recordsTimeout = 15 * time.Second
	// TODO CHECK
	maxRecordSize = 1000 * 1024 // The maximum size of a record sent to Kinesis Firehose, before base64-encoding, is 1024 KB
	// TODO maxBattchRecords = 500
	maxBatchRecords = 500     // The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller. This limit cannot be changed.
	maxBatchSize    = 4 << 20 // 4 MiB per call

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
	srv        *Server
	buff       *bytebufferpool.ByteBuffer
	lines      int
	batch      []*bytebufferpool.ByteBuffer
	batchSize  int
	records    []types.Record
	done       chan bool
	flushed    chan bool
	finish     chan bool
	ID         int64
	t          *time.Timer
	onFlyRetry int64
}

// NewClient creates a new client that connects to a Firehose
func NewClient(srv *Server) *Client {
	n := atomic.AddInt64(&clientCount, 1)
	log.Printf("Firehose client %s [%d]: starting", srv.cfg.StreamName, n)

	clt := &Client{
		done:    make(chan bool),
		finish:  make(chan bool),
		flushed: make(chan bool),
		srv:     srv,
		ID:      n,
		t:       time.NewTimer(recordsTimeout),
		batch:   make([]*bytebufferpool.ByteBuffer, 0, maxBatchRecords),
		records: make([]types.Record, 0, maxBatchRecords),
		buff:    pool.Get(),
	}
	clt.batch = append(clt.batch, clt.buff)

	go clt.listen()

	return clt
}

func (clt *Client) listen() {
	log.Printf("Firehose client %s [%d]: ready", clt.srv.cfg.StreamName, clt.ID)
	for {
		select {
		case ri := <-clt.srv.C:

			var r []byte
			if clt.srv.cfg.Serializer != nil {
				var err error
				if r, err = clt.srv.cfg.Serializer(ri); err != nil {
					log.Printf("Firehose client %s [%d]: ERROR serializer: %s", clt.srv.cfg.StreamName, clt.ID, err)
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
				log.Printf("Firehose client %s [%d]: ERROR: one record is over the limit %d/%d", clt.srv.cfg.StreamName, clt.ID, recordSize, maxRecordSize)
				continue
			}

			clt.lines++

			// The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller. This limit cannot be changed.
			// if clt.count >= clt.srv.cfg.MaxRecords || len(clt.batch) >= maxBatchRecords || clt.batchSize+recordSize+1 >= maxBatchSize {
			// TODO CHECK
			if len(clt.batch) >= clt.srv.cfg.MaxRecords || clt.exceedsMaxBatchSize(recordSize) {
				// log.Printf("#%d flushing maxBatchSize/MaxBatchRecords: lines %d/%d | batch %d/%d | size [%d B] %d/%d kiB",
				// 	clt.ID,
				// 	clt.lines, clt.srv.cfg.MaxConcatLines,
				// 	len(clt.batch), clt.srv.cfg.MaxRecords,
				// 	recordSize, clt.totalBatchSize()/1024, maxBatchSize/1024)
				// Force flush
				clt.flush()
			}

			// The maximum size of a record sent to Kinesis Firehose, before base64-encoding, is 1000 KB.
			if !clt.srv.cfg.ConcatRecords || clt.buff.Len()+recordSize+1 >= maxRecordSize || clt.lines >= clt.srv.cfg.MaxConcatLines {
				if clt.buff.Len() > 0 {
					// Save in new record
					// TODO debug print
					// log.Printf("#%d Appending record %d/%d  size %d/%d kiB, line count %d/%d batchsize %d/%d kiB",
					// 	clt.ID,
					// 	len(clt.batch), clt.srv.cfg.MaxRecords,
					// 	clt.buff.Len()/1024, maxRecordSize/1024,
					// 	clt.lines, clt.srv.cfg.MaxConcatLines,
					// 	clt.totalBatchSize()/1024, maxBatchSize/1024,
					// )
					clt.batchSize += clt.buff.Len()
					clt.buff = pool.Get()
					clt.batch = append(clt.batch, clt.buff)
					clt.lines = 0 // TODO CHECK the meaning of this count. Now is lines count
				}
			}

			clt.buff.Write(r)
			clt.buff.Write(newLine)

			// log.Printf("ID %d Wrote record to buff - batch record num #%d/%d, line count %d/%d recordsize %d/%dkiB batchsize %d/%d kiB err %d\n",
			// 	clt.ID,
			// 	len(clt.batch), maxBatchRecords,
			// 	clt.count, clt.srv.cfg.MaxRecords,
			// 	clt.buff.Len()/1024, maxRecordSize/1024,
			// 	clt.totalBatchSize()/1024, maxBatchSize/1024,
			// 	clt.srv.errors)

			// if len(clt.batch)+1 >= maxBatchRecords || clt.batchSize >= maxBatchSize {
			// 	log.Println("Flushing matxbatChrecords/maxBatchSize")
			// 	clt.flush()
			// }

		case <-clt.t.C:
			clt.flush()

		case f := <-clt.finish:
			//Stop and drain the timer channel
			if f && !clt.t.Stop() {
				select {
				case <-clt.t.C:
				default:
				}
			}

			var err error
			err = clt.flush()

			if err != nil {
				log.Printf("Error flushing on finish: %s", err)
			}

			if f {
				// Have to finish
				if l := clt.batchRecords(); l > 0 {
					log.Printf("Firehose client %s [%d]: Exit, %d records lost", clt.srv.cfg.StreamName, clt.ID, l)
					clt.done <- false // WARN: To avoid blocking the processs
					return
				}

				log.Printf("Firehose client %s [%d]: Exit", clt.srv.cfg.StreamName, clt.ID)
				clt.done <- true
				return
			}

			// Only a flush
			if l := len(clt.batch); l > 0 || err != nil {
				log.Printf("Firehose client %s [%d]: Flush, %d records pending", clt.srv.cfg.StreamName, clt.ID, l)
				clt.flushed <- false // WARN: To avoid blocking the processs
			} else {
				clt.flushed <- true
			}
		}
	}
}

func (clt *Client) exceedsMaxBatchSize(recordSize int) bool {
	return clt.totalBatchSize()+recordSize+1 >= maxBatchSize
}

func (clt *Client) totalBatchSize() int {
	return clt.batchSize + clt.buff.Len()
}

func (clt *Client) batchRecords() int {

	if clt.totalBatchSize() == 0 {
		return 0
	}

	return len(clt.batch)
}

// flush build the last record if need and send the records slice to AWS Firehose
func (clt *Client) flush() error {

	if !clt.t.Stop() {
		select {
		case <-clt.t.C:
		default:
		}
	}
	clt.t.Reset(recordsTimeout)

	// Don't send empty batch
	if clt.totalBatchSize() == 0 {
		return nil
	}

	if clt.srv.awsSvc == nil {
		log.Println("flush error: firehose client not ready")
		clt.srv.failure()
		return errors.New("flush error: firehose client not ready")
	}

	// Create slice with the struct need by firehose
	for _, b := range clt.batch {
		clt.records = append(clt.records, types.Record{
			Data: b.B,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	// Send the batch
	output, err := clt.srv.awsSvc.PutRecordBatch(ctx, &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(clt.srv.cfg.StreamName),
		Records:            clt.records,
	})
	log.Printf("Firehose client %s [%d]: DEBUG: flushed: lines %d/%d | batch %d/%d | size %d/%d B (err %v)\n",
		clt.srv.cfg.StreamName, clt.ID,
		clt.lines, clt.srv.cfg.MaxConcatLines,
		len(clt.records), clt.srv.cfg.MaxRecords,
		clt.totalBatchSize(), maxBatchSize,
		err)

	if err != nil {
		if clt.srv.cfg.OnFHError != nil {
			clt.srv.cfg.OnFHError(err)
		}
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "ThrottlingException" {
			log.Printf("Firehose client %s [%d]: ERROR ThrottlingException: %s", clt.srv.cfg.StreamName, clt.ID, err)
		} else {
			log.Printf("Firehose client %s [%d]: ERROR PutRecordBatch: %s", clt.srv.cfg.StreamName, clt.ID, err)
		}
		clt.srv.failure()

		// Sleep few millisecond because is a failure
		time.Sleep(globalFailureWait)

		// Send back to the buffer
		for i := range clt.batch {
			// The limit of retry elements will be applied just to non-critical messages
			if !clt.srv.cfg.Critical && atomic.LoadInt64(&clt.onFlyRetry) > onFlyRetryLimit {
				log.Printf("Firehose client %s [%d]: ERROR maximum of batch records retrying (%d): %s",
					clt.srv.cfg.StreamName, clt.ID, onFlyRetryLimit, err)
				continue
			}

			// Sending back to channel, it will run a goroutine
			log.Println("sending back to channel")
			clt.retry(clt.batch[i].B)
		}
	} else if *output.FailedPutCount > 0 {
		log.Printf("Firehose client %s [%d]: partial failed, %d sent back to the buffer", clt.srv.cfg.StreamName, clt.ID, *output.FailedPutCount)
		// Sleep few millisecond because the partial failure
		time.Sleep(partialFailureWait)

		for i, r := range output.RequestResponses {
			if r.ErrorCode == nil {
				continue
			}

			if clt.srv.cfg.OnFHError != nil {
				clt.srv.cfg.OnFHError(errors.New(*r.ErrorMessage))
			}

			// The limit of retry elements will be applied just to non-critical messages
			if !clt.srv.cfg.Critical && atomic.LoadInt64(&clt.onFlyRetry) > onFlyRetryLimit {
				log.Printf("Firehose client %s [%d]: ERROR maximum of batch records retrying %d, %s %s",
					clt.srv.cfg.StreamName, clt.ID, onFlyRetryLimit, *r.ErrorCode, *r.ErrorMessage)
				continue
			}

			if *r.ErrorCode == firehoseError {
				log.Printf("Firehose client %s [%d]: ERROR in AWS: %s - %s", clt.srv.cfg.StreamName, clt.ID, *r.ErrorCode, *r.ErrorMessage)
			}

			// Sending back to channel, it will run a goroutine
			clt.retry(clt.batch[i].B)
		}
	}

	// Put slice bytes in the pull after sent
	for _, b := range clt.batch {
		pool.Put(b)
	}

	clt.lines = 0
	clt.batchSize = 0
	clt.batch = clt.batch[:0]
	clt.records = clt.records[:0]
	clt.buff = pool.Get()
	clt.batch = append(clt.batch, clt.buff)

	return err
}

// Exit finish the go routine of the client
func (clt *Client) Exit() {
	clt.finish <- true
	<-clt.done
}

func (clt *Client) retry(orig []byte) {
	if len(orig) == 0 {
		return
	}

	// Remove the last byte, is a newLine
	b := make([]byte, len(orig)-len(newLine))
	copy(b, orig[:len(orig)-len(newLine)])

	go func(b []byte) {
		atomic.AddInt64(&clt.onFlyRetry, 1)
		defer atomic.AddInt64(&clt.onFlyRetry, -1)
		var sent, exit bool
		for {
			// As this is a goroutine, we need to avoid a race cond of client exiting and closed channel
			clt.srv.Lock()
			if !clt.srv.exiting {
				select {
				case clt.srv.C <- b:
					sent = true
				default:
				}
			}
			exit = clt.srv.exiting
			clt.srv.Unlock()
			if sent {
				log.Printf("Firehose client %s [%d]: retry: record sent", clt.srv.cfg.StreamName, clt.ID)
				return
			}
			if exit {
				log.Printf("Firehose client %s [%d]: retry: client exiting, discarding", clt.srv.cfg.StreamName, clt.ID)
				return
			}
			log.Printf("Firehose client %s [%d]: retry: channel full, waiting", clt.srv.cfg.StreamName, clt.ID)
			<-time.After(1 * time.Second)
		}
	}(b)
}
