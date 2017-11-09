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

	chReload chan bool
	chDone   chan bool
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
		chDone:   make(chan bool),
		chReload: make(chan bool),
	}

	go srv._reload()

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

	if srv.cfg.Workers == 0 {
		srv.cfg.Workers = defaultWorkers
	}

	if srv.cfg.MaxRecords == 0 {
		srv.cfg.MaxRecords = defaultMaxRecords
	}

	log.Printf("Firehose config: %#v", srv.cfg)

	srv.chReload <- true

	return nil
}

// Exit terminate all clients and close the channels
func (srv *Server) Exit() {

	srv.Lock()
	srv.exiting = true
	srv.Unlock()

	close(srv.chReload)

	for _, c := range srv.clients {
		c.Exit()
	}

	if len(srv.C) > 0 {
		log.Printf("Firehose: messages lost %d", len(srv.C))
	}

	close(srv.C)

	// finishing the server
	srv.chDone <- true
}

func (srv *Server) isExiting() bool {
	srv.Lock()
	defer srv.Unlock()

	if srv.exiting {
		return true
	}

	return false
}

// Waiting to the server if is running
func (srv *Server) Waiting() {
	if srv.chDone == nil {
		return
	}

	<-srv.chDone
	close(srv.chDone)
}
