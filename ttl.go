package gocache

import "time"

func isExpired(expiredTime int64) bool {
	return expiredTime < time.Now().Unix()
}
