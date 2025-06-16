package firehosepool

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace = "firehosepool"
)

var (
	metricFlushesByTrigger = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "flushes_by_trigger",
		Help:      "Flushes by trigger source (timer, cron, max-size, max-records, finish)",
	}, []string{"stream", "trigger"})
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
		Buckets:   []float64{1, 5, 20, 100, 250, 400},
	}, []string{"stream"})
	metricWorkers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "workers",
		Help:      "Number of firehosepool workers",
	}, []string{"stream"})
	metricChannelPercentBusy = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "channel_busy_pct",
		Help:      "Percentage of receiving channel busy",
	}, []string{"stream"})
)

func registerMetrics() {
	prometheus.Register(metricFlushesByTrigger)
	prometheus.Register(metricFlushErrors)
	prometheus.Register(metricFlushThrottled)
	prometheus.Register(metricFlushPartialFailed)
	prometheus.Register(metricRecordRetry)
	prometheus.Register(metricBatchSizeKiB)
	prometheus.Register(metricBatchRecords)
	prometheus.Register(metricWorkers)
	prometheus.Register(metricChannelPercentBusy)
}

func unRegisterMetrics() {
	prometheus.Unregister(metricFlushesByTrigger)
	prometheus.Unregister(metricFlushErrors)
	prometheus.Unregister(metricFlushThrottled)
	prometheus.Unregister(metricFlushPartialFailed)
	prometheus.Unregister(metricRecordRetry)
	prometheus.Unregister(metricBatchSizeKiB)
	prometheus.Unregister(metricBatchRecords)
	prometheus.Unregister(metricWorkers)
	prometheus.Unregister(metricChannelPercentBusy)
}

func cleanMetrics(stream string) {
	metricWorkers.DeleteLabelValues(stream)
	metricChannelPercentBusy.DeleteLabelValues(stream)
}
