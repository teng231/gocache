package gocache

import "sync"

type hub struct {
	s    []*string
	lock *sync.RWMutex
}

func (h *hub) getS() []*string {
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.s
}

func (h *hub) pop() string {
	if h.len() == 0 {
		return ""
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	out := h.s[0]
	h.s[0] = nil
	h.s = h.s[1:]
	return *out
}

func (h *hub) push(items ...string) {
	h.lock.Lock()
	defer h.lock.Unlock()
	for _, item := range items {
		h.s = append(h.s, &item)
	}
}

func (h *hub) len() int {
	// h.lock.RLock()
	// defer h.lock.RUnlock()
	return len(h.s)
}

func (h *hub) remove(key string) bool {
	if h.len() == 0 {
		return true
	}
	index := -1
	h.lock.RLock()
	for i, val := range h.s {
		if val == nil {
			continue
		}
		if *val == key {
			index = i
			break
		}
	}
	h.lock.RUnlock()
	if index != -1 {
		h.lock.Lock()
		defer h.lock.Unlock()
		copy(h.s[index:], h.s[index+1:])
		h.s[len(h.s)-1] = nil  // Erase last element (write zero value).
		h.s = h.s[:len(h.s)-1] // Truncate slice.
		return true
	}
	return false
}
