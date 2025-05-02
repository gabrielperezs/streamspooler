package firehosepool

import (
	"fmt"
	"log/slog"
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
		srv.clientsReset()
	}
}

func (srv *Server) failure() {
	if srv.isExiting() {
		return
	}

	srv.Lock()
	if time.Since(srv.lastError) > errorsFrame {
		srv.errors = 0
	}
	srv.errors++
	reload := srv.errors > maxErrors
	srv.lastError = time.Now()
	srv.Unlock()

	slog.Info("Firehosepool failure marked", "stream", srv.cfg.StreamName, "errors", srv.errors, "reload", reload)

	if reload {
		select {
		case srv.chReload <- true:
		default:
		}
	}
}

func (srv *Server) fhClientReset(cfg *Config) (err error) {
	if srv.lastConnection.Add(limitIntervalConnection).Before(time.Now()) {
		slog.Info("Firehosepool: creating new aws firehose client", "stream", cfg.StreamName)

		var fhcg ClientGetter
		if cfg.FHClientGetter != nil {
			fhcg = cfg.FHClientGetter
		} else {
			fhcg = &FHClientGetter{}
		}

		srv.awsSvc, err = fhcg.GetClient(cfg)
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

func (srv *Server) clientsReset() {
	srv.Lock()
	defer srv.Unlock()

	defer func() {
		slog.Info("Firehosepool: workers reset done", "stream", srv.cfg.StreamName, "workers", len(srv.clients), "in-queue", fmt.Sprintf("%d/%d", len(srv.C), cap(srv.C)))
	}()

	currClients := len(srv.clients)

	// No changes in the number of clients
	if currClients == srv.cliDesired {
		return
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
	metricWorkers.WithLabelValues(srv.cfg.StreamName).Set(float64(len(srv.clients)))
}
