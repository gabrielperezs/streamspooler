# streamspooler
Workers pooler for Amazon Data Firehose with buffering, timers flushing and limits control.

# Project that is using this package

* https://github.com/gallir/smart-relayer/ - A light and smart cache proxy in Golang that reduces latency in client applications

# Example for Firehose

```golang
fh, err := firehosePool.New(firehosePool.Config{
		StreamName:    "mystream",
		MaxWorkers:    10,
})
fh.C <- []byte("This a test message")
```