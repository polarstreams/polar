package utils

import (
	"sync"
	"sync/atomic"
)

// CopyOnWriteMap provides basic functionality of a copy-on-write dictionary
// that uses a valueCreator function (instead of a value like sync.Map)
type CopyOnWriteMap struct {
	m  atomic.Value // Map
	mu sync.Mutex
}

func NewCopyOnWriteMap() *CopyOnWriteMap {
	c := &CopyOnWriteMap{
		m:  atomic.Value{},
		mu: sync.Mutex{},
	}

	c.m.Store(make(map[interface{}]interface{}))
	return c
}

func (c *CopyOnWriteMap) LoadOrStore(key interface{}, valueCreator func() (interface{}, error)) (value interface{}, loaded bool, err error) {
	existingMap := c.m.Load().(map[interface{}]interface{})
	if v, ok := existingMap[key]; ok {
		return v, true, nil
	}

	defer c.mu.Unlock()
	c.mu.Lock()

	// Re check after acquiring the lock
	existingMap = c.m.Load().(map[interface{}]interface{})
	if v, ok := existingMap[key]; ok {
		return v, true, nil
	}

	// Shallow copy existing
	newMap := make(map[interface{}]interface{}, len(existingMap))
	for k, v := range existingMap {
		newMap[k] = v
	}

	newValue, err := valueCreator()

	if err != nil {
		return nil, false, err
	}

	newMap[key] = newValue
	c.m.Store(newMap)

	return newValue, false, nil
}
