package firehosepool

import (
	"fmt"
	"log"
	"time"
)

const (
	connectionRetry         = 2 * time.Second
	connectTimeout          = 15 * time.Second
	errorsFrame             = 10 * time.Second
	maxErrors               = 10 // Limit of errors to restart the connection
	limitIntervalConnection = 5 * time.Second
)

func (srv *Server) _reload() {
	for range srv.chReload {
		if srv.isExiting() {
			continue
		}

		if err := srv.clientsReset(); err != nil {
			log.Printf("Firehose ERROR: can't connect to kinesis: %s", err)
			time.Sleep(connectionRetry)
		}
	}
}

func (srv *Server) failure() {
	log.Println("MARK FAILURE")
	if srv.isExiting() {
		return
	}

	srv.Lock()
	defer srv.Unlock()

	if time.Since(srv.lastError) > errorsFrame {
		srv.errors = 0
	}

	srv.errors++
	srv.lastError = time.Now()
	log.Printf("Firehose: %d errors detected", srv.errors)

	if srv.errors > maxErrors {
		select {
		case srv.chReload <- true:
		default:
		}
	}
}

func (srv *Server) fhClientReset(cfg *Config) (err error) {
	if srv.lastConnection.Add(limitIntervalConnection).Before(time.Now()) {
		log.Printf("creating new firehose client for the stream %s ...", srv.cfg.StreamName)

		srv.awsSvc, err = srv.Fhcg.GetClient(cfg)
		if err != nil {
			err = fmt.Errorf("firehose init error: %w", err)
			srv.errors++
			srv.lastError = time.Now()
			return
		}
		srv.lastConnection = time.Now()
	}
	return
}

func (srv *Server) clientsReset() (err error) {
	srv.Lock()
	defer srv.Unlock()
	log.Println("Firehose: RELOADING clients unlocked")

	defer func() {
		log.Printf("Firehose %s clients %d, in the queue %d/%d", srv.cfg.StreamName, len(srv.clients), len(srv.C), cap(srv.C))
	}()

	currClients := len(srv.clients)

	// No changes in the number of clients
	if currClients == srv.cliDesired {
		return nil
	}

	// If the config define lower number than the active clients remove the difference
	if currClients > srv.cliDesired {
		toExit := currClients - srv.cliDesired
		for i := 0; i < toExit; i++ {
			go srv.clients[0].Exit() // Don't block waiting for the client to flush
			srv.clients[0] = nil
			srv.clients = srv.clients[1:]
		}
	} else {
		// If the config define higher number than the active clients start new clients
		for i := currClients; i < srv.cliDesired; i++ {
			srv.clients = append(srv.clients, NewClient(srv))
		}
	}

	return nil
}
