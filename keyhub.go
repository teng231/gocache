package gocache

import (
	"bytes"
)

type hub struct {
	s []*[]byte
}

func (h *hub) pop() []byte {
	if h.len() == 0 {
		return nil
	}
	// log.Print(h.s)
	out := h.s[0]
	h.s[0] = nil
	h.s = h.s[1:]
	return *out
}

func (h *hub) push(items ...[]byte) {
	for _, item := range items {
		h.s = append(h.s, &item)
	}
}

func (h *hub) len() int {
	return len(h.s)
}

func (k *hub) remove(key []byte) bool {
	if k.len() == 0 {
		return true
	}
	index := -1
	for i, val := range k.s {
		if val == nil {
			continue
		}
		if bytes.Equal(*val, key) {
			index = i
			break
		}
	}
	if index != -1 {
		copy(k.s[index:], k.s[index+1:])
		k.s[len(k.s)-1] = nil  // Erase last element (write zero value).
		k.s = k.s[:len(k.s)-1] // Truncate slice.
		return true
	}
	return false
}
