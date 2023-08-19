package gocache

// type hub struct {
// 	s    []*string
// 	lock *sync.RWMutex
// }

// func (h *hub) getS() []*string {
// 	h.lock.Lock()
// 	defer h.lock.Unlock()
// 	return h.s
// }

// func (h *hub) pop() string {
// 	if h.len() == 0 {
// 		return ""
// 	}
// 	h.lock.Lock()
// 	defer h.lock.Unlock()
// 	out := h.s[0]
// 	h.s[0] = nil
// 	h.s = h.s[1:]
// 	return *out
// }

// func (h *hub) push(item string) {
// 	h.lock.Lock()
// 	defer h.lock.Unlock()
// 	h.s = append(h.s, &item)
// }

// func (h *hub) len() int {
// 	// h.lock.RLock()
// 	// defer h.lock.RUnlock()
// 	return len(h.s)
// }

// func remove(keys []*string, key string) ([]*string, bool) {
// 	if len(keys) == 0 {
// 		return keys, false
// 	}
// 	for i, val := range keys {
// 		if val == nil {
// 			continue
// 		}
// 		if *val == key {
// 			// log.Print("removed: ", key)
// 			copy(keys[i:], keys[i+1:])
// 			keys[len(keys)-1] = nil   // Erase last element (write zero value).
// 			keys = keys[:len(keys)-1] // Truncate slice.
// 			return keys, true
// 		}
// 	}
// 	return keys, false
// }
// func pop(keys []*string) ([]*string, *string) {
// 	if len(keys) == 0 {
// 		return keys, nil
// 	}
// 	out := keys[0]
// 	keys[0] = nil
// 	keys2 := append([]*string{}, keys[1:]...)
// 	return keys2, out
// }

// -------------- standalone code ---------

func removeV2(keys []*string, key string) ([]*string, bool) {
	if len(keys) == 0 {
		return keys, false
	}
	for i, val := range keys {
		if val == nil {
			continue
		}
		if *val == key {
			keys2, _ := popV2(keys, i)
			return keys2, true
		}
	}
	return keys, false
}

func popV2(slice []*string, index int) ([]*string, *string) {
	if index < 0 || index >= len(slice) {
		return slice, nil // index out of range
	}
	element := slice[index]
	return append(slice[:index], slice[index+1:]...), element
}
