package cron

import (
	"sync"
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
	once sync.Once
}

// New creates a cron timer that ticks each hour at d minutes and seconds
// if d is greater than one hour, it uses only minutes and seconds
func New(d time.Duration) *Cron {
	// get only minutes ad seconds
	if h := d.Truncate(time.Hour); h > 0 {
		d = d - h
	}

	c := &Cron{
		C:    make(chan time.Time),
		done: make(chan struct{}),
		d:    d,
	}
	// The timer is created before starting the goroutine, so Stop can read
	// t.t without racing with start.
	c.t = time.NewTimer(c.next(time.Now()))
	go c.start()
	return c
}

func (c *Cron) start() {
	for {
		select {
		case n := <-c.t.C:
			// Give up the tick if Stop is called while nobody is reading C,
			// otherwise this goroutine would leak.
			select {
			case c.C <- n:
			case <-c.done:
				return
			}
			c.t.Reset(c.next(time.Now()))
		case <-c.done:
			return
		}
	}
}

// Stop halts the cron. It is safe to call more than once, but only the first
// call reports whether the pending tick was stopped before firing.
// C is never closed, so a stopped cron simply never ticks again.
func (c *Cron) Stop() bool {
	stopped := false
	c.once.Do(func() {
		stopped = c.t.Stop()
		close(c.done)
	})
	return stopped
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
