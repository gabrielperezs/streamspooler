package bigqueryPool

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gallir/bytebufferpool"
)

const (
	recordsTimeout  = 15 * time.Second
	maxBatchRecords = 10000

	partialFailureWait = 200 * time.Millisecond
	globalFailureWait  = 500 * time.Millisecond
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
	count       int
	batch       []interface{}
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
		batch:  make([]interface{}, 0, maxBatchRecords),
	}

	go clt.listen()

	return clt
}

func (clt *Client) listen() {
	log.Printf("BigQuery client %s [%d]: ready", clt.srv.cfg.DataSet, clt.ID)
	for {
		select {
		case ri := <-clt.srv.C:
			clt.count++
			if clt.count >= clt.srv.cfg.MaxRecords || len(clt.batch) >= maxBatchRecords {
				clt.flush()
			}

			schema, _ := bigquery.InferSchema(ri)
			log.Printf("%+v", schema)
			row := bigquery.ValuesSaver{}
			row.Schema = schema
			for _, e := range ri.(map[string]interface{}) {
				row.Row = append(row.Row, e)
			}

			clt.batch = append(clt.batch, ri)
		case <-clt.t.C:
			clt.flush()
		case <-clt.finish:
			//Stop and drain the timer channel
			if !clt.t.Stop() {
				select {
				case <-clt.t.C:
				default:
				}
			}
			clt.flush()
			log.Printf("BigQuery client %s [%d]: Exit", clt.srv.cfg.DataSet, clt.ID)
			clt.done <- true
			return
		}
	}
}

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

	tableName := time.Now().Format(clt.srv.cfg.TableName)
	inserter := clt.srv.bigqueryCli.Dataset(clt.srv.cfg.DataSet).Table(tableName).Uploader()
	if err := inserter.Put(context.Background(), clt.batch); err != nil {
		switch e := err.(type) {
		case bigquery.PutMultiError:
			for i, rowInsertionError := range e {
				log.Printf("BigQuery error line: %s", clt.batch[i])
				for _, err := range rowInsertionError.Errors {
					log.Printf("BigQuery err %s", err)
				}
			}
		default:
			log.Panicf("BigQuery error: dataset:%s table:%s - %+v", clt.srv.cfg.DataSet, clt.srv.cfg.TableName, err)
		}
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
