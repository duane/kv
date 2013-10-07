package kv

import (
  "code.google.com/p/goprotobuf/proto"
)

func (key *Key) Equals(other interface{}) bool {
  other_key := other.(*Key)
  return key.GetVersion() == other_key.GetVersion() && key.GetKey() == other_key.GetKey()
}

func (key *Key) Less(other interface{}) bool {
  other_key := other.(*Key)
  key_name := key.GetKey()
  other_key_name := other_key.GetKey()
  if key_name < other_key_name {
    return true
  }
  if key_name > other_key_name {
    return false
  }
  return key.GetVersion() < other_key.GetVersion()
}

func (key *Key) MarshalKey() (data []byte, err error) {
  data, err = proto.Marshal(key)
  return
}

func (key *Key) UnmarshalKey(data []byte) error {
  return proto.Unmarshal(data, key)
}
