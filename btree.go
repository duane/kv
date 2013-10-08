package kv

import (
  "github.com/cznic/b"
)

type BTree b.Tree

type StringKey string

func (k *StringKey) Equals(other interface{}) bool {
  return string(*k) == string(*(other.(*StringKey)))
}

func (k *StringKey) Less(other interface{}) bool {
  return string(*k) < string(*(other.(*StringKey)))
}

func (k *StringKey) String() string {
  return string(*k)
}

func KeyCmp(key_a_, key_b_ interface{}) int {
  key_a := key_a_.(Key)
  key_b := key_b_.(Key)
  if key_a.Equals(key_b) {
    return 0
  }
  if key_a.Less(key_b) {
    return -1
  }
  return 1
}

func NewBTree() *BTree {
  tree := (*BTree)(b.TreeNew(b.Cmp(KeyCmp)))
  return tree
}

func (t *BTree) Get(key_ string) (value []byte, err error) {
  return
}

func (t *BTree) Put(key string, value []byte) (err error) {
  return
}

func (t *BTree) Del(key string) (err error) {
  return
}
