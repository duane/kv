package kv

import (
  "errors"
  "sstable"
)

type MemTable interface {
  Get(key string) (value []byte, err error)
  Put(key string, value []byte) (err error)
  Del(key string) (err error)
}

type HashMemTable map[string][]byte

var RowNotFoundError = errors.New("Row not found")

func (h *HashMemTable) Get(key string) (value []byte, err error) {
  value, ok := (*h)[key]
  if !ok {
    err = RowNotFoundError
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
  pair_chan := make(chan sstable.Pair)

  go sstable.EncodePairStream(filename, pair_chan)
  for k, v := range *h {
    pair := sstable.Pair{Key: []byte(k), Value: v}
    pair_chan <- pair
  }
  return
}

func (h *HashMemTable) Read(filename string) (err error) {
  pair_chan := make(chan *sstable.Pair)
  go sstable.DecodePairStream(filename, pair_chan)
  for {
    pair, ok := <-pair_chan
    if !ok {
      break
    }
    err = h.Put(string(pair.Key), pair.Value)
    if err != nil {
      return
    }
  }
  return
}
