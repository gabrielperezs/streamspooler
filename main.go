package firehosePool

import (
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/firehose"
)

const (
	defaultBufferSize = 1024
	defaultWorkers    = 2
	defaultMaxRecords = 500
)

// Config is the general configuration for the server
type Config struct {
	Workers       int
	Buffer        int
	ConcatRecords bool // Contact many rows in one firehose record
	MaxRecords    int  // To send in batch to Kinesis
	Compress      bool

	StreamName string // Kinesis/Firehose stream name
	Region     string // AWS region
	Profile    string // AWS Profile name
}

type Server struct {
	sync.Mutex

	cfg     Config
	C       chan []byte
	clients []*Client

	done     chan bool
	failing  bool
	reseting bool
	exiting  bool

	awsSvc         *firehose.Firehose
	lastConnection time.Time
	lastError      time.Time
	errors         int64
}

// New create a pool of workers
func New(cfg Config) *Server {

	srv := &Server{
		done: make(chan bool),
	}

	srv.Reload(&cfg)

	return srv
}

// Reload the configuration
func (srv *Server) Reload(cfg *Config) (err error) {
	srv.Lock()
	defer srv.Unlock()

	srv.cfg = *cfg

	if srv.cfg.Buffer == 0 {
		srv.cfg.Buffer = defaultBufferSize
	}

	srv.C = make(chan []byte, srv.cfg.Buffer)

	if srv.cfg.Workers == 0 {
		srv.cfg.Workers = defaultWorkers
	}

	if srv.cfg.MaxRecords == 0 {
		srv.cfg.MaxRecords = defaultMaxRecords
	}

	log.Printf("C: %#v", srv.cfg)

	go srv.retry()

	return nil
}

func (srv *Server) Exit() {
	srv.exiting = true

	for _, c := range srv.clients {
		c.Exit()
	}

	log.Printf("Firehose: messages lost %d", len(srv.C))

	// finishing the server
	srv.done <- true
}

func (srv *Server) Waiting() {
	<-srv.done
}
