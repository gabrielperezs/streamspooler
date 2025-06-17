# streamspooler Changelog

## [2.0.2] - 2025-06-17

### Fixed

- Clean stream gauge metric values on exit.

## [2.0.1] - 2025-05-07

### Fixed

- Some logs where referencing a wrong StreamName variable.
- fix maxRecordSize counting the return carriage concat by one byte.
- fix flush counting totalBatchSize to check records not flushed.
- Added missing CHANGELOG version links.

### Changed

- Use metric Workers by label instead of stream name, just like other metrics.
- Do not write newLine when ConcatRecords=false

## [2.0.0] - 2025-04-04

### Added

- Added FlushCron option to set an hourly timer that flushes hourly at a specified minute and second. Defaults hourly at 59m30s
- Added FlushTimeout to control the max time between flushes. Defaults to 15m. It was fixed to this default.
- Added tests for throttling error.
- Added FHGetter option to inject a firehose client getter. Used for testing.
- Added Prometheus metrics, disabled by default. Config options EnableMetrics and MetricsName.

### Changed

- Changed major version to v2
- firehosepool.New now returns also an error, allowing failing fast at runtime if there are errors with the firehose client setup.
- Workers reload separated from firehose reload. Also fixes reload problem when firehose was failing.
- Changed aws-sdk-go to v2
- Max batch size was 3 MiB. The correct max supported is 4MiB
- Max Record size was 1000 KB. The correct one is 1000 KiB
- monad package copied to this repos at streamspooler/v2/monad
- MaxRecords now refers to the Max Batch size records. Hard aws limit is 500. 500 is the default and max
- Added MaxConcatLines to control the max lines concatenated by '\n' in a Record. Defaults to 500. There is no limit on aws for this.
- Log changed to slog. Log appends and buf writes optional (can be verbose), disabled by default.

### Fixed

- Total batch size calculation was wrong. It was adding the whole record size for each record line. So it did not use the 3MiB batch limit.
- panic race condition on exit when retrying failed records. retry goroutine could send to a closed channel
- MaxRecords was a wrong mix of max lines per record and maxrecords. It was kind of limiting both, but not reseting on each append.
- flushing without appending  for a long buf record with max lines or size did not flush the record so it would be appended and then fail. Fixed making buffer belong to a batch (buff added to batch just after buf init), so the current buffer is also flushed. This also makes flushing in other parts (timer, exit) easier.
- MinWorkers was not being used
- compress introduced magic prefix so not snappy compatible
- Monad resources now are freed on Reload() if no longer needed (min and max workers are equal).

### Removed

- Removed streamspooler/kinesis data streams

[2.0.2]: https://github.com/gabrielperezs/streamspooler/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/gabrielperezs/streamspooler/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/gabrielperezs/streamspooler/compare/v1.0.0...v2.0.0
