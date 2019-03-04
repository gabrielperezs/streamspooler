package pubsubPool

import (
	"context"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

const (
	connectionRetry         = 2 * time.Second
	connectTimeout          = 15 * time.Second
	errorsFrame             = 10 * time.Second
	maxErrors               = 10 // Limit of errors to restart the connection
	limitIntervalConnection = 30 * time.Second
)

func (srv *Server) _reload() {
	for range srv.chReload {
		if srv.isExiting() {
			continue
		}

		if err := srv.clientsReset(); err != nil {
			log.Printf("PubSub ERROR: can't connect to kinesis: %s", err)
			time.Sleep(connectionRetry)
		}
	}
}

func (srv *Server) failure() {
	if srv.isExiting() {
		return
	}

	srv.Lock()
	defer srv.Unlock()

	if time.Now().Sub(srv.lastError) > errorsFrame {
		srv.errors = 0
	}

	srv.errors++
	srv.lastError = time.Now()
	log.Printf("PubSub: %d errors detected", srv.errors)

	if srv.errors > maxErrors {
		select {
		case srv.chReload <- true:
		default:
		}
	}
}

func (srv *Server) clientsReset() (err error) {
	srv.Lock()
	defer srv.Unlock()

	if srv.errors == 0 && srv.lastConnection.Add(limitIntervalConnection).Before(time.Now()) {
		log.Printf("PubSub Reload config to the stream %s", srv.cfg.Project)

		ctx := context.Background()
		var authClientOption option.ClientOption
		if srv.cfg.GCEAPIKey != "" {
			authClientOption = option.WithAPIKey(srv.cfg.GCEAPIKey)
		}
		if srv.cfg.GCECredentialsFile != "" {
			authClientOption = option.WithCredentialsFile(srv.cfg.GCECredentialsFile)
		}

		var err error
		srv.pubsubCli, err = pubsub.NewClient(ctx, srv.cfg.Project, authClientOption)
		if err != nil {
			log.Printf("PubSub ERROR: session: %s", err)

			srv.errors++
			srv.lastError = time.Now()
			return err
		}

		log.Printf("PubSub Connected to %s", srv.cfg.Project)
		srv.lastConnection = time.Now()
		srv.errors = 0
	}

	defer func() {
		log.Printf("PubSub %s clients %d, in the queue %d/%d", srv.cfg.Project, len(srv.clients), len(srv.C), cap(srv.C))
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
