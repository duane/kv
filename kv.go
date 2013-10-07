package kv

import (
  "errors"
  _ "log"
  "sstable"
)

type MemTable interface {
  Get(key string) (value []byte, err error)
  Put(key string, value []byte) (err error)
  Del(key string) (err error)
}

type HashMemTable map[string][]byte

var KeyNotFoundError = errors.New("Key not found")

func (h *HashMemTable) Get(key string) (value []byte, err error) {
  value, ok := (*h)[key]
  if !ok {
    err = KeyNotFoundError
  }
  return
}

func (h *HashMemTable) Put(key string, value []byte) (err error) {
  (*h)[key] = value
  return
}

func (h *HashMemTable) Del(key string) (err error) {
  delete(*h, key)
  return
}

func (h *HashMemTable) Flush(filename string) (err error) {
  pair_chan := make(chan *sstable.Pair)

  go sstable.EncodePairStream(filename, pair_chan)
  var prev *sstable.Pair
  prev = prev
  for k, v := range *h {
    version := uint64(0)
    //log.Print(k, v)
    pair := &sstable.Pair{Key: &Key{Key: &k, Version: &version}, Value: v}
    pair_chan <- pair
    <-pair_chan
  }
  close(pair_chan)
  return
}

func ctor() (key sstable.Key) {
  k := ""
  v := uint64(0)
  key = &Key{Key: &k, Version: &v}
  return
}

func (h *HashMemTable) Read(filename string) (err error) {
  pair_chan := make(chan *sstable.Pair)

  go sstable.DecodePairStream(ctor, filename, pair_chan)
  for {
    pair, ok := <-pair_chan
    if !ok {
      break
    }
    err = h.Put(pair.Key.(*Key).GetKey(), pair.Value)
    if err != nil {
      return
    }
  }
  return
}
