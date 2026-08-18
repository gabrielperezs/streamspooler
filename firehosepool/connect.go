package firehosepool

import (
	"fmt"
	"log/slog"
	"time"
)

const (
	connectTimeout          = 15 * time.Second
	errorsFrame             = 10 * time.Second
	maxErrors               = 10 // Limit of errors to restart the connection
	limitIntervalConnection = 5 * time.Second
)

func (srv *Server) _reload() {
	for range srv.chReload {
		srv.clientsReset()
	}
}

func (srv *Server) failure() {
	srv.Lock()
	if srv.exiting {
		srv.Unlock()
		return
	}

	if time.Since(srv.lastError) > errorsFrame {
		srv.errors = 0
	}
	srv.errors++
	errCount := srv.errors
	srv.lastError = time.Now()
	stream := srv.cfg.StreamName

	// The signal goes out under the lock: Exit sets exiting under it before
	// closing chReload, so this either sent before Exit took the lock or it
	// does not send at all, which is what keeps a failure during teardown from
	// sending on a closed channel. The default case keeps the send from ever
	// waiting, whether or not _reload is reading.
	reload := errCount > maxErrors
	if reload {
		select {
		case srv.chReload <- true:
		default:
		}
	}
	srv.Unlock()

	slog.Info("Firehosepool failure marked", "stream", stream, "errors", errCount, "reload", reload)
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

	// Exit snapshots the workers under this same lock and then closes srv.C: a
	// worker created after that snapshot would be left reading from a closed
	// channel. Either this runs first and the new workers are in Exit's
	// snapshot, or it does not run at all.
	if srv.exiting {
		return
	}

	defer func() {
		slog.Info("Firehosepool: workers reset done", "stream", srv.cfg.StreamName, "workers", len(srv.clients), "in-queue", fmt.Sprintf("%d/%d", len(srv.C), cap(srv.C)))
	}()

	currClients := len(srv.clients)
	cliDesired := int(srv.cliDesired.Load())

	// No changes in the number of clients
	if currClients == cliDesired {
		return
	}

	// If the config define lower number than the active clients remove the difference
	if currClients > cliDesired {
		toExit := currClients - cliDesired
		for range toExit {
			go srv.clients[0].Exit() // Don't block waiting for the client to flush
			srv.clients[0] = nil
			srv.clients = srv.clients[1:]
		}
	} else {
		// If the config define higher number than the active clients start new clients
		for i := currClients; i < cliDesired; i++ {
			srv.clients = append(srv.clients, NewClient(srv))
		}
	}
	metricWorkers.WithLabelValues(srv.cfg.Label).Set(float64(len(srv.clients)))
}
