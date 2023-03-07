# GoCache

Big Problems, if you build a system/app/service and want to cache data in memory. But your data usually insert/update/delete, memory increase and not reduce in spite of you call `delete`  function data inside map

I try using `bigcache`, `freecache` but it's not work memory so high after 10 days, and my pod restart by OOM Kill. Then i create this lib for resolve all issue

## Install


```bash
go get github.com/teng231/gocache
```

## Refer
- [https://teivah.medium.com/maps-and-memory-leaks-in-go-a85ebe6e7e69](https://teivah.medium.com/maps-and-memory-leaks-in-go-a85ebe6e7e69)