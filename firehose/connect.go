package firehosePool

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
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
			log.Printf("Firehose ERROR: can't connect to kinesis: %s", err)
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

func (srv *Server) clientsReset() (err error) {
	srv.Lock()
	defer srv.Unlock()

	if srv.errors == 0 && srv.lastConnection.Add(limitIntervalConnection).Before(time.Now()) {
		log.Printf("Firehose Reload config to the stream %s", srv.cfg.StreamName)

		optFns := []func(*config.LoadOptions) error{
			config.WithRegion(srv.cfg.Region),
		}
		if srv.cfg.Endpoint != "" {
			optFns = append(optFns, config.WithBaseEndpoint(srv.cfg.Endpoint))
		}

		if srv.cfg.Profile != "" {
			optFns = append(optFns, config.WithSharedConfigProfile(srv.cfg.Profile))
		}

		cfg, err := config.LoadDefaultConfig(context.TODO(), optFns...)
		if err != nil {
			log.Printf("Firehose Config error %s", err)
			srv.errors++
			srv.lastError = time.Now()
			return err
		}

		srv.awsSvc = firehose.NewFromConfig(cfg)
		stream := &firehose.DescribeDeliveryStreamInput{
			DeliveryStreamName: &srv.cfg.StreamName,
		}

		var l *firehose.DescribeDeliveryStreamOutput
		l, err = srv.awsSvc.DescribeDeliveryStream(context.TODO(), stream)
		if err != nil {
			log.Printf("Firehose ERROR: describe stream: %s", err)

			srv.errors++
			srv.lastError = time.Now()
			return err
		}

		log.Printf("Firehose Connected to %s (%s) status %s",
			*l.DeliveryStreamDescription.DeliveryStreamName,
			*l.DeliveryStreamDescription.DeliveryStreamARN,
			l.DeliveryStreamDescription.DeliveryStreamStatus)

		srv.lastConnection = time.Now()
		srv.errors = 0
	}

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
