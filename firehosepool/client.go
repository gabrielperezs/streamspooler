package firehosepool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/smithy-go"
	"github.com/gabrielperezs/streamspooler/v2/internal/compress"
	"github.com/gabrielperezs/streamspooler/v2/internal/cron"
	"github.com/gallir/bytebufferpool"
)

const (
	maxRecordSize   = 1000 * 1024 // The maximum size of a record sent to Data Firehose, before base64-encoding, is 1024 KB
	maxBatchRecords = 500         // The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller. This limit cannot be changed.
	maxBatchSize    = 4 << 20     // 4 MiB per call

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
	cron       *cron.Cron
	onFlyRetry int64
}

// NewClient creates a new client that connects to a Firehose
func NewClient(srv *Server) *Client {
	n := atomic.AddInt64(&clientCount, 1)

	clt := &Client{
		done:    make(chan bool),
		finish:  make(chan bool),
		flushed: make(chan bool),
		srv:     srv,
		ID:      n,
		t:       time.NewTimer(srv.cfg.FlushTimeout),
		cron:    cron.New(srv.cfg.FlushCron),
		batch:   make([]*bytebufferpool.ByteBuffer, 0, maxBatchRecords),
		records: make([]types.Record, 0, maxBatchRecords),
		buff:    pool.Get(),
	}
	clt.batch = append(clt.batch, clt.buff)

	go clt.listen()

	return clt
}

func (clt *Client) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int64("ID", clt.ID),
		slog.String("Stream", clt.srv.cfg.StreamName),
	)
}

func (clt *Client) listen() {
	slog.Info("Firehosepool: worker ready", "worker", clt)
	for {
		select {
		case ri := <-clt.srv.C:

			var r []byte
			if clt.srv.cfg.Serializer != nil {
				var err error
				if r, err = clt.srv.cfg.Serializer(ri); err != nil {
					slog.Error("serializer error", "err", err, "worker", clt)
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
				slog.Error("firehose ERROR: one record is over the limitd", "worker", clt, "recordSize", recordSize, "maxRecordSize", maxRecordSize)
				continue
			}

			clt.lines++

			// The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller. This limit cannot be changed.
			// force flush if any limit reached
			if len(clt.batch) >= clt.srv.cfg.MaxRecords {
				metricFlushesMaxRecords.WithLabelValues(clt.srv.cfg.MetricLabel).Inc()
				clt.flush()
			} else if clt.exceedsMaxBatchSize(recordSize) {
				metricFlushesMaxSize.WithLabelValues(clt.srv.cfg.MetricLabel).Inc()
				clt.flush()
			}

			// The maximum size of a record sent to Kinesis Firehose, before base64-encoding, is 1000 KB.
			if !clt.srv.cfg.ConcatRecords || clt.buff.Len()+recordSize+1 >= maxRecordSize || clt.lines >= clt.srv.cfg.MaxConcatLines {
				if clt.buff.Len() > 0 {
					// Create new record on batch
					clt.batchSize += clt.buff.Len()
					if clt.srv.cfg.LogBatchAppend {
						slog.Info("Firehosepool: appending record to batch",
							"worker", clt,
							"record-size", fmt.Sprintf("%d/%dB", clt.buff.Len(), maxRecordSize),
							"record-lines", fmt.Sprintf("%d/%d", clt.lines, clt.srv.cfg.MaxConcatLines),
							"batch-records", fmt.Sprintf("%d/%d", len(clt.batch)+1, clt.srv.cfg.MaxRecords),
							"batch-size", fmt.Sprintf("%d/%dB", clt.totalBatchSize(), maxBatchSize))
					}
					clt.buff = pool.Get()
					clt.batch = append(clt.batch, clt.buff)
					clt.lines = 0
				}
			}

			clt.buff.Write(r)
			clt.buff.Write(newLine)

			if clt.srv.cfg.LogRecordWrite {
				slog.Info("Firehosepool wrote to buf", "worker", clt,
					"entry size", recordSize,
					"line count", fmt.Sprintf("%d/%d", clt.lines, clt.srv.cfg.MaxConcatLines),
					"record count", fmt.Sprintf("%d/%d", len(clt.batch), clt.srv.cfg.MaxRecords),
					"record size", fmt.Sprintf("%d/%dB", clt.buff.Len(), maxRecordSize),
					"batch size", fmt.Sprintf("%d/%dB", clt.totalBatchSize(), maxBatchSize))
			}

		case <-clt.t.C:
			metricFlushesTime.WithLabelValues(clt.srv.cfg.MetricLabel).Inc()
			clt.flush()
		case <-clt.cron.C:
			metricFlushesTime.WithLabelValues(clt.srv.cfg.MetricLabel).Inc()
			clt.flush()

		case f := <-clt.finish:
			//Stop the timers
			// drain no longer needed as of go 1.23. See Stop() docs.
			if f {
				clt.t.Stop()
				clt.cron.Stop()
			}

			var err error
			err = clt.flush()

			if err != nil {
				slog.Error("Firehosepool error flushing", "err", err, "worker", clt)
			}

			if f {
				// Have to finish
				if l := clt.batchRecords(); l > 0 {
					slog.Info("Firehosepool worker exit", "worker", clt, "records-lost", l)
					clt.done <- false // WARN: To avoid blocking the processs
					return
				}
				slog.Info("Firehosepool worker exit", "worker", clt)
				clt.done <- true
				return
			}

			// Only a flush
			if l := len(clt.batch); l > 0 || err != nil {
				slog.Error("Firehosepool worker flush error", "worker", clt, "records-pending", l, "err", err)
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

	// no longer needed Stop and drain after go 1.23
	clt.t.Reset(clt.srv.cfg.FlushTimeout)

	// Don't send empty batch
	if clt.totalBatchSize() == 0 {
		return nil
	}

	if clt.srv.awsSvc == nil {
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
	if clt.srv.cfg.LogFlushes {
		slog.Info("Firehosepool worker flushed",
			"worker", clt,
			"batch-records", fmt.Sprintf("%d/%d", len(clt.batch), clt.srv.cfg.MaxRecords),
			"batch-size", fmt.Sprintf("%d/%dB", clt.totalBatchSize(), maxBatchSize),
			"err", err)
	}
	if err == nil {
		metricBatchRecords.WithLabelValues(clt.srv.cfg.MetricLabel).Observe(float64(len(clt.batch)))
		metricBatchSizeKiB.WithLabelValues(clt.srv.cfg.MetricLabel).Observe(float64(clt.totalBatchSize()) / 1024)
	}

	if err != nil {
		if clt.srv.cfg.OnFHError != nil {
			clt.srv.cfg.OnFHError(err)
		}
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "ThrottlingException" {
			slog.Error("Firehosepool flush: firehose  Throttling error", "err", err, "worker", clt)
			metricFlushThrottled.WithLabelValues(clt.srv.cfg.MetricLabel).Inc()
		} else {
			slog.Error("Firehosepool flush: firehose PutRecordBatch error", "err", err, "worker", clt)
			metricFlushErrors.WithLabelValues(clt.srv.cfg.MetricLabel).Inc()
		}
		clt.srv.failure()

		// Sleep few millisecond because is a failure
		time.Sleep(globalFailureWait)

		// Send back to the buffer
		for i := range clt.batch {
			// The limit of retry elements will be applied just to non-critical messages
			if !clt.srv.cfg.Critical && atomic.LoadInt64(&clt.onFlyRetry) > onFlyRetryLimit {
				slog.Error("Firehosepool worker flush: max batch records retrying limit error", "worker", clt, "onFlyRetryLimit", onFlyRetryLimit)
				continue
			}

			// Sending back to channel, it will run a goroutine
			clt.retry(clt.batch[i].B)
		}
	} else if *output.FailedPutCount > 0 {
		slog.Info("Firehosepool worker flush: firehose partial failure, records sent back to the buffer", "worker", clt, "failed-put-count", *output.FailedPutCount)
		metricFlushPartialFailed.WithLabelValues(clt.srv.cfg.MetricLabel).Add(float64(*output.FailedPutCount))
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
				slog.Error("Firehosepool worker flush: max batch records retrying limit error", "worker", clt, "onFlyRetryLimit", onFlyRetryLimit)
				continue
			}

			if *r.ErrorCode == firehoseError {
				slog.Error("Firehosepool AWS Firehose error", "worker", clt, "error-code", *r.ErrorCode, "error-msg", *r.ErrorMessage)
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
	metricRecordRetry.WithLabelValues(clt.srv.cfg.MetricLabel).Inc()

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
				slog.Debug("Firehosepool worker retry: record sent", "worker", clt)
				return
			}
			if exit {
				slog.Info("Firehosepool worker retry: worker exiting, discarding", "worker", clt)
				return
			}
			slog.Debug("Firehosepool worker retry: channel full, waiting", "worker", clt)
			<-time.After(1 * time.Second)
		}
	}(b)
}
