package firehosepool

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace = "firehosepool"
)

var (
	metricFlushesTime = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "flushes_by_time",
		Help:      "Flushes triggered by max time or cron",
	}, []string{"stream"})
	metricFlushesMaxSize = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "flushes_by_max_size",
		Help:      "Flushes triggered by max batch size",
	}, []string{"stream"})
	metricFlushesMaxRecords = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "flushes_by_max_records",
		Help:      "Flushes triggered by max num records per batch",
	}, []string{"stream"})
	metricFlushErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "flush_errors",
		Help:      "firehose PutRecordBatch throttle errors",
	}, []string{"stream"})
	metricFlushThrottled = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "flush_throttled",
		Help:      "firehose PutRecordBatch errors",
	}, []string{"stream"})
	metricFlushPartialFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "flush_partial_failed_records",
		Help:      "firehose PutRecordBatch partial errors",
	}, []string{"stream"})
	metricRecordRetry = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "record_retry",
		Help:      "records sent to retry",
	}, []string{"stream"})
	metricBatchSizeKiB = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "batch_size_kib",
		Help:      "flush batch size in KiB",
		Buckets:   prometheus.ExponentialBuckets(5, 3, 7), // 5 to 3645 KiB. 4MiB (max) would be +inf
	}, []string{"stream"})
	metricBatchRecords = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "batch_records",
		Help:      "flush batch size in KiB",
		Buckets:   prometheus.ExponentialBuckets(1, 2.8, 7), // 1 to 481.89. 500 (max) would be +inf
	}, []string{"stream"})
)

func registerMetrics() {
	prometheus.Register(metricFlushesTime)
	prometheus.Register(metricFlushesMaxSize)
	prometheus.Register(metricFlushesMaxRecords)
	prometheus.Register(metricFlushErrors)
	prometheus.Register(metricFlushThrottled)
	prometheus.Register(metricFlushPartialFailed)
	prometheus.Register(metricRecordRetry)
	prometheus.Register(metricBatchSizeKiB)
	prometheus.Register(metricBatchRecords)
}

func unRegisterMetrics() {
	prometheus.Unregister(metricFlushesTime)
	prometheus.Unregister(metricFlushesMaxSize)
	prometheus.Unregister(metricFlushesMaxRecords)
	prometheus.Unregister(metricFlushErrors)
	prometheus.Unregister(metricFlushThrottled)
	prometheus.Unregister(metricFlushPartialFailed.MetricVec)
	prometheus.Unregister(metricRecordRetry)
	prometheus.Unregister(metricBatchSizeKiB)
	prometheus.Unregister(metricBatchRecords)
}
