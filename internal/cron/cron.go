package cron

import (
	"time"
)

// This is a fixed houry cron timer
// changing period to 1 Minute would behave as a minute cron
const period = time.Hour

type Cron struct {
	C    chan time.Time
	t    *time.Timer
	d    time.Duration
	done chan struct{}
}

// New creates a cron timer that ticks each hour at d minutes and seconds
// if d is greater than one hour, it uses only minutes and seconds
func New(d time.Duration) *Cron {

	// get only minutes ad seconds
	if h := d.Truncate(time.Hour); h > 0 {
		d = d - h
	}

	t := &Cron{
		C:    make(chan time.Time),
		done: make(chan struct{}),
		d:    d,
	}
	go t.start()
	return t
}

func (c *Cron) start() {
	c.t = time.NewTimer(c.next(time.Now()))
	for {
		select {
		case n := <-c.t.C:
			c.C <- n
			c.t.Reset(c.next(time.Now()))
		case <-c.done:
			return
		}
	}
}

func (c *Cron) Stop() bool {
	s := c.t.Stop()
	close(c.C)
	close(c.done)
	return s
}

// next retuns the duration to the nex tick
// ticks each hour at d duration minutes and seconds
// duration must be less than 1h
func (c *Cron) next(n time.Time) time.Duration {
	next := n.Truncate(period).Add(c.d)
	if n.Sub(next) > 0 {
		next = next.Add(period)
	}
	d := next.Sub(n)
	// fmt.Println("Received", n, "Setting next at: ", next, "duration: ", d)
	return d
}
