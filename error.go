package gocache

const (
	E_not_found              = "not_found"
	E_not_found_clean_window = "not_found_clean_window"
	E_invalid_ttl            = "invalid_ttl"
	E_shard_size_invalid     = "shard size need > 0 and <= 256"
	E_queue_is_empty         = "queue is empty"
	E_not_found_keymap       = "not_found_keymap"
	E_not_found_expired      = "not_found_expired"
)
