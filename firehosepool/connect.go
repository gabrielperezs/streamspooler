package firehosePool

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
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

	if time.Now().Sub(srv.lastError) > errorsFrame {
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

		var sess *session.Session

		if srv.cfg.Profile != "" {
			sess, err = session.NewSessionWithOptions(session.Options{
				Profile:           srv.cfg.Profile,
				SharedConfigState: session.SharedConfigEnable,
			})
		} else {
			sess, err = session.NewSession()
		}

		if err != nil {
			log.Printf("Firehose ERROR: session: %s", err)

			srv.errors++
			srv.lastError = time.Now()
			return err
		}

		config := &aws.Config{Region: aws.String(srv.cfg.Region)}
		if srv.cfg.Endpoint != "" {
			config.Endpoint = aws.String(srv.cfg.Endpoint)
		}

		srv.awsSvc = firehose.New(sess, config)
		stream := &firehose.DescribeDeliveryStreamInput{
			DeliveryStreamName: &srv.cfg.StreamName,
		}

		var l *firehose.DescribeDeliveryStreamOutput
		l, err = srv.awsSvc.DescribeDeliveryStream(stream)
		if err != nil {
			log.Printf("Firehose ERROR: describe stream: %s", err)

			srv.errors++
			srv.lastError = time.Now()
			return err
		}

		log.Printf("Firehose Connected to %s (%s) status %s",
			*l.DeliveryStreamDescription.DeliveryStreamName,
			*l.DeliveryStreamDescription.DeliveryStreamARN,
			*l.DeliveryStreamDescription.DeliveryStreamStatus)

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
