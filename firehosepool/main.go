package firehosepool

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/gabrielperezs/streamspooler/v2/monad"
)

const (
	defaultBufferSize      = 1024
	defaultWorkers         = 1
	defaultMaxWorkers      = 10
	defaultMaxLines        = 500
	defaultThresholdWarmUp = 0.6
	defaultCoolDownPeriod  = 15 * time.Second
)

// Config is the general configuration for the server
type Config struct {
	// Internal clients details
	MinWorkers      int
	MaxWorkers      int
	ThresholdWarmUp float64
	Interval        time.Duration
	CoolDownPeriod  time.Duration
	Critical        bool // Handle this stream as critical
	Serializer      func(i interface{}) ([]byte, error)
	FHClientGetter  ClientGetter // Allow injecting a custom firehose client getter. Mostly for mock testing

	// Limits
	Buffer         int
	ConcatRecords  bool // Contact many rows in one firehose record
	MaxConcatLines int  // Max Concat Lines per record. Default 500
	MaxRecords     int  // Max records per batch. Max 500 (firehose hard limit), default 500
	Compress       bool // Compress records with snappy

	// Authentication and enpoints
	StreamName string // Kinesis/Firehose stream name
	Region     string // AWS region
	Profile    string // AWS Profile name
	Endpoint   string // AWS endpoint

	OnFHError func(e error)
}

type Server struct {
	sync.Mutex

	cfg        Config
	C          chan interface{}
	clients    []*Client
	cliDesired int

	monad *monad.Monad

	chReload chan bool
	chDone   chan bool
	exiting  bool

	awsSvc         *firehose.Client
	lastConnection time.Time
	lastError      time.Time
	errors         int64
}

// New create a pool of workers
func New(cfg Config) (*Server, error) {

	if cfg.Buffer == 0 {
		cfg.Buffer = defaultBufferSize
	}
	srv := &Server{
		chDone:   make(chan bool, 1),
		chReload: make(chan bool, 1),
		C:        make(chan interface{}, cfg.Buffer),
	}

	go srv._reload()

	err := srv.Reload(&cfg)
	if err != nil {
		return nil, err
	}

	return srv, nil
}

// Reload the configuration
func (srv *Server) Reload(cfg *Config) (err error) {
	srv.Lock()
	defer srv.Unlock()

	if err = srv.fhClientReset(cfg); err != nil {
		log.Printf("Reload aborted due to firehose client error: %s", err)
		return
	}
	srv.cfg = *cfg

	if srv.cfg.MaxWorkers == 0 {
		srv.cfg.MaxWorkers = defaultMaxWorkers
	}

	if srv.cfg.MinWorkers == 0 {
		srv.cfg.MinWorkers = 1
	}

	if srv.cfg.ThresholdWarmUp == 0 {
		srv.cfg.ThresholdWarmUp = defaultThresholdWarmUp
	}

	if srv.cfg.CoolDownPeriod.Nanoseconds() == 0 {
		srv.cfg.CoolDownPeriod = defaultCoolDownPeriod
	}

	if srv.cfg.MaxConcatLines == 0 {
		srv.cfg.MaxConcatLines = defaultMaxLines
	}

	if srv.cfg.MaxRecords == 0 {
		srv.cfg.MaxRecords = maxBatchRecords
	}

	if srv.cfg.MaxRecords > 500 {
		srv.cfg.MaxRecords = 500
	}

	if srv.cfg.MaxWorkers > srv.cfg.MinWorkers {
		monadCfg := &monad.Config{
			Min:            uint64(srv.cfg.MinWorkers),
			Max:            uint64(srv.cfg.MaxWorkers),
			Interval:       srv.cfg.Interval,
			CoolDownPeriod: srv.cfg.CoolDownPeriod,
			WarmFn: func() bool {
				if srv.cliDesired == 0 {
					return true
				}

				l := float64(len(srv.C))
				if l == 0 {
					return false
				}

				currPtc := (l / float64(cap(srv.C))) * 100
				log.Printf("Firehose WarmFn: %d/%d (%.2f%%)", len(srv.C), cap(srv.C), currPtc)

				return currPtc > srv.cfg.ThresholdWarmUp*100
			},
			DesireFn: func(n uint64) {
				srv.cliDesired = int(n)
				select {
				case srv.chReload <- true:
				default:
				}
			},
		}

		if srv.monad == nil {
			srv.monad = monad.New(monadCfg)
		} else {
			go srv.monad.Reload(monadCfg)
		}
	} else {
		srv.cliDesired = srv.cfg.MaxWorkers
		// for len(srv.clients) < srv.cliDesired {
		// 	srv.clients = append(srv.clients, NewClient(srv))
		// }
	}

	log.Printf("Firehose config: %#v", srv.cfg)

	select {
	case srv.chReload <- true:
	default:
	}

	return nil
}

// Flush terminate all clients and close the channels
func (srv *Server) Flush() (err error) {
	if srv.isExiting() {
		return nil
	}

	for _, c := range srv.clients {
		c.finish <- false // It will flush
	}

	for _, c := range srv.clients {
		f := <-c.flushed
		if !f {
			return errors.New("flushed failed")
		}
	}

	return
}

// Exit terminate all clients and close the channels
func (srv *Server) Exit() {
	srv.Lock()
	if srv.exiting {
		srv.Unlock()
		return
	}
	srv.exiting = true
	srv.Unlock()

	if srv.monad != nil {
		srv.monad.Exit()
	}

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
	return srv.exiting
}

// Waiting to the server if is running
func (srv *Server) Waiting() {
	if srv.chDone == nil {
		return
	}

	<-srv.chDone
	close(srv.chDone)
}
