package jevmetricsconnector

import (
	"container/list"
	"time"
)

// scoreCache is guarded by connectorImp.mu. Eviction always makes a metric
// unscored, so it is retained until a fresh assessment becomes available.
type scoreCache struct {
	limit   int
	entries map[string]*list.Element
	order   *list.List
}

type cacheEntry struct {
	key   string
	score metricScore
}

func newScoreCache(limit int) *scoreCache {
	return &scoreCache{limit: limit, entries: make(map[string]*list.Element), order: list.New()}
}

func (c *scoreCache) remove(e *list.Element) {
	delete(c.entries, e.Value.(cacheEntry).key)
	c.order.Remove(e)
}

func (c *scoreCache) get(key string, now time.Time, ttl time.Duration) (metricScore, bool) {
	e, ok := c.entries[key]
	if !ok {
		return metricScore{}, false
	}
	s := e.Value.(cacheEntry).score
	if now.Sub(s.ScoredAt) >= ttl {
		c.remove(e)
		return metricScore{}, false
	}
	c.order.MoveToFront(e)
	return s, true
}

func (c *scoreCache) put(key string, s metricScore) {
	if e, ok := c.entries[key]; ok {
		e.Value = cacheEntry{key, s}
		c.order.MoveToFront(e)
		return
	}
	if c.order.Len() >= c.limit {
		c.remove(c.order.Back())
	}
	c.entries[key] = c.order.PushFront(cacheEntry{key, s})
}

func (c *scoreCache) prune(now time.Time, ttl time.Duration) {
	for _, e := range c.entries {
		if now.Sub(e.Value.(cacheEntry).score.ScoredAt) >= ttl {
			c.remove(e)
		}
	}
}
