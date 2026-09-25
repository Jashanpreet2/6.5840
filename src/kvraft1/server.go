package kvraft

import (
	"log"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	mu      sync.Mutex
	storage map[string]ValueVersion
}

type PutReq struct {
	Key     string
	Value   string
	Version rpc.Tversion
}

type GetReq struct {
	Key string
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if getReq, ok := req.(GetReq); ok {
		if v, ok := kv.storage[getReq.Key]; ok {
			return v
		}
		return rpc.ErrNoKey
	} else if putReq, ok := req.(PutReq); ok {
		if _, ok := kv.storage[putReq.Key]; !ok && putReq.Version > 0 {
			return rpc.ErrNoKey
		}
		if v, ok := kv.storage[putReq.Key]; ok && v.Version != putReq.Version {
			return rpc.ErrVersion
		}
		kv.storage[putReq.Key] = ValueVersion{putReq.Value, putReq.Version + 1}
		return rpc.OK
	} else {
		log.Panicf("req is neither GetReq nor PutReq: %v. Type: %T", req, req)
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	reply.Value, reply.Version = "", 0

	err, res := kv.rsm.Submit(GetReq{Key: args.Key})
	if err != rpc.OK {
		reply.Err = err
	} else if res == rpc.ErrNoKey {
		reply.Err = rpc.ErrNoKey
	} else if res, ok := res.(ValueVersion); ok {
		reply.Err = rpc.OK
		reply.Value = res.Value
		reply.Version = res.Version
	} else {
		log.Panicf("Get res is not errnokey, errwrongleader, or value version struct. Res: %v. Type: %T", res, res)
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	// version must be 0 if key doesnt exist
	// if version is greater than 0 and key doenst exist return errnokey
	err, res := kv.rsm.Submit(PutReq{Key: args.Key, Value: args.Value, Version: args.Version})
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else if err, ok := res.(string); ok {
		reply.Err = rpc.Err(err)
	} else {
		log.Panicf("res doesn't convert to string: %v. Type: %T", res, res)
	}
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(PutReq{})
	labgob.Register(GetReq{})

	kv := &KVServer{me: me,
		mu: sync.Mutex{}, storage: map[string]ValueVersion{},
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
