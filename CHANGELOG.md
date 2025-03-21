# streamspooler Changelog

## [Unreleased]  


### Added

- Added FlushCron option to set an hourly timer at flushes hourly at a specified minute and second. Defaults hourly at 55m30s
- Added FlushTimeout to control the max time between flushes. Defaults to 15m. It was fixed to this default.
- Added tests for throttling error.

### Changed
- firehosepool.New now returns also an error, allowing failing fast at runtime if there are errorrs with the firehose client setup.
- Changed aws-sdk-go to v2
- Max batch size was 3 MiB. The correct max supported is 4MiB
- Max Record size was 1000 KB. The correct one is 1000 KiB
- Monad copied to an internal package

### Fixed
- Total batch size calculation was wrong. It was adding the whole record size for each record line. So it did not use the 3MiB batch limit.
- panic race condition on exit when retrying failed records. retry goroutine could send to a closed channel
- MaxRecords was a wrong mix of max lines per record and maxrecords. It was kind of limiting both, but not reseting on each append.
- flushing without appending  for a long buf record with max lines or size did not flush the record so it would be appended and then fail. Fixed making buffer belong to a batch (buff added to batch just after buf init), so the current buffer is also flushed. This also makes flushing in other parts (timer, exit) easier.
- MinWorkers was not being used
- compress introduced magic prefix so not snappy compatible


### Removed

- Removed streamspooler/kinesis data streams
