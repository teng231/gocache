package gocache

import "bytes"

type hub struct {
	s [][]byte
}

func (h *hub) pop() []byte {
	if h.len() == 0 {
		return nil
	}
	if len(h.s) == 0 {
		return nil
	}
	val := h.s[0]
	h.s = h.s[1:]
	return val
}
func (h *hub) push(item ...[]byte) {
	h.s = append(h.s, item...)
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
		if bytes.Equal(val, key) {
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
