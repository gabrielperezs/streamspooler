package firehosepool

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
)

type ClientGetter interface {
	GetClient(cfg *Config) (*firehose.Client, error)
}

type FHClientGetter struct{}

func (c *FHClientGetter) GetClient(cfg *Config) (*firehose.Client, error) {
	optFns := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}
	if cfg.Endpoint != "" {
		optFns = append(optFns, config.WithBaseEndpoint(cfg.Endpoint))
	}

	if cfg.Profile != "" {
		optFns = append(optFns, config.WithSharedConfigProfile(cfg.Profile))
	}

	awscfg, err := config.LoadDefaultConfig(context.TODO(), optFns...)
	if err != nil {
		return nil, fmt.Errorf("firehose GetClient ERROR: load config: %w", err)
	}

	cli := firehose.NewFromConfig(awscfg)
	stream := &firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: &cfg.StreamName,
	}

	var l *firehose.DescribeDeliveryStreamOutput
	l, err = cli.DescribeDeliveryStream(context.TODO(), stream)
	if err != nil {
		return cli, fmt.Errorf("firehose GetClient ERROR: describe stream: %w", err)
	}

	slog.Info("Firehosepool: Firehose Connected",
		"stream-name", *l.DeliveryStreamDescription.DeliveryStreamName,
		"stream-arn", *l.DeliveryStreamDescription.DeliveryStreamARN,
		"status", l.DeliveryStreamDescription.DeliveryStreamStatus)

	return cli, nil
}
