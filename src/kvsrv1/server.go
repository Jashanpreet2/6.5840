package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	storage map[string]struct {
		value   string
		version rpc.Tversion
	}
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{mu: sync.Mutex{}, storage: map[string]struct {
		value   string
		version rpc.Tversion
	}{}}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.storage[args.Key]; ok {
		reply.Err = rpc.OK
		reply.Value = v.value
		reply.Version = v.version
		return
	}
	reply.Value, reply.Version = "", 0
	reply.Err = rpc.ErrNoKey
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// version must be 0 if key doesnt exist
	// if version is greater than 0 and key doenst exist return errnokey
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.storage[args.Key]; !ok && args.Version > 0 {
		reply.Err = rpc.ErrNoKey
		return
	}
	if v, ok := kv.storage[args.Key]; ok && v.version != args.Version {
		DPrintf("Given version: %v, actual version: %v\n", args.Version, v.version)
		reply.Err = rpc.ErrVersion
		return
	}
	kv.storage[args.Key] = struct {
		value   string
		version rpc.Tversion
	}{args.Value, args.Version + 1}
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
