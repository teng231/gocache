# GoCache

Big Problems, if you build a system/app/service and want to cache data in memory. But your data usually insert/update/delete, memory increase and not reduce in spite of you call `delete`  function data inside map

I try using `bigcache`, `freecache` but it's not work memory so high after 10 days, and my pod restart by OOM Kill. Then i create this lib for resolve all issue

## Installation

```bash
go get github.com/teng231/gocache
```

## Usage

```go
engine, _ := New(&Config{
		Shard:       64,
		OnRemove:    nil,
		CleanWindow: time.Minute,
		TTL:         12 * time.Hour,
	})

if err := engine.Set("item1", []byte(`{"id":1, "name":"item1"}`)); err!=nil {
    log.Print(err)
}
out, err := engine.Get("item1")
if err!=nil{
    log.Print(err)
}

log.Print("OUT:", string(out))

out, err = engine.Pop()

log.Print("OUT:", string(out))
```

## Refer
- [https://teivah.medium.com/maps-and-memory-leaks-in-go-a85ebe6e7e69](https://teivah.medium.com/maps-and-memory-leaks-in-go-a85ebe6e7e69)
- [https://github.com/golang/go/issues/35890](https://github.com/golang/go/issues/35890)