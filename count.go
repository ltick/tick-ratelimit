// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3 with static-linking exception.
// See LICENCE file for details.

// The ratelimit package provides an efficient token bucket implementation
// that can be used to limit the rate of arbitrary things.
// See http://en.wikipedia.org/wiki/Token_bucket.
package ratelimit

import (
	"sync"
	"time"
    "strconv"
	"log"
)

// Bucket represents a token bucket that fills at a predetermined rate.
// Methods on Bucket may be called concurrently.
type Counter struct {
	startTime time.Time
	capacity  int64

	// The mutex guards the fields following it.
	mu sync.Mutex

	// avail holds the number of available tokens
	// in the bucket, as of availTick ticks from startTime.
	// It will be negative when there are consumers
	// waiting for tokens.
	avail int64
	task  []*CounterTask
}

type CounterTask struct {
	count   int64
	endTime time.Time
	result  chan bool
}

// NewCounter returns a new token bucket that fills at the
// rate of one token every fillInterval, up to the given
// maximum capacity. Both arguments must be
// positive. The bucket is initially full.
func NewCounter(retryInterval time.Duration, capacity int64) *Counter {
	if capacity <= 0 {
		panic("token bucket capacity is not > 0")
	}
	c := &Counter{
		startTime: time.Now(),
		capacity:  capacity,
		avail:     capacity,
		task:      make([]*CounterTask, 0, 1),
	}
	observer := func() {
		ticker := time.NewTicker(retryInterval)
		for {
			select {
			case <-ticker.C:
                c.mu.Lock()
				c.adjust()
                c.mu.Unlock()
			}
		}
	}
	go observer()

	return c
}

// Take takes count tokens from the bucket without blocking. It returns
// the time that the caller should wait until the tokens are actually
// available.
//
// Note that if the request is irrevocable - there is no way to return
// tokens to the bucket once this method commits us to taking them.
func (c *Counter) Take(count int64) bool {
	return c.take(count, infinityDuration)
}

// TakeMaxDuration is like Take, except that
// it will only take tokens from the bucket if the wait
// time for the tokens is no greater than maxWait.
//
// If it would take longer than maxWait for the tokens
// to become available, it does nothing and reports false,
// otherwise it returns the time that the caller should
// wait until the tokens are actually available, and reports
// true.
func (c *Counter) TakeMaxDuration(count int64, maxWait time.Duration) bool {
	return c.take(count, maxWait)
}

// Available returns the number of available tokens. It will be negative
// when there are consumers waiting for tokens. Note that if this
// returns greater than zero, it does not guarantee that calls that take
// tokens from the buffer will succeed, as the number of available
// tokens could have changed in the meantime. This method is intended
// primarily for metrics reporting and debugging.
func (c *Counter) Available() int64 {
	return c.available()
}

// available is the internal version of available - it takes the current time as
// an argument to enable easy testing.
func (c *Counter) available() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.avail
}

func (c *Counter) BringAvailable(count int64) bool {
	c.mu.Lock()
	c.avail += count
	if c.avail > c.capacity {
		c.avail = c.capacity
	}
	c.adjust()
    c.mu.Unlock()

	return true
}

// Capacity returns the capacity that the bucket was created with.
func (c *Counter) Capacity() int64 {
	return c.capacity
}

// take is the internal version of Take - it takes the current time as
// an argument to enable easy testing.
func (c *Counter) take(count int64, maxWait time.Duration) (ok bool) {
	if count <= 0 {
		return true
	}
	if count > c.capacity {
		return false
	}
	c.mu.Lock()
	avail := c.avail - count
	if avail >= 0 {
		c.avail = avail
        c.mu.Unlock()
		return true
	}
	task := &CounterTask{
		count:   count,
		endTime: time.Now().Add(maxWait),
		result:  make(chan bool),
	}
	c.task = append(c.task, task)
	c.mu.Unlock()
	select {
	case ok = <-task.result:
		return ok
	}
	return false
}

// adjust adjusts timeout task based on the current time.
func (c *Counter) adjust() {
    tmpTask := make([]*CounterTask, 0, 1)
	for _, task := range c.task {
		if task != nil {
			if time.Now().After(task.endTime) {
				task.result <- false
				continue
			} else {
				if c.avail >= task.count {
					c.avail = c.avail - task.count
					task.result <- true
					continue
				}
			}
			tmpTask = append(tmpTask, task)
		}
	}
	c.task = tmpTask
	if len(tmpTask) > 0 {
		log.Println("task wait queue: " + strconv.Itoa(len(tmpTask)))
	}
	return
}
