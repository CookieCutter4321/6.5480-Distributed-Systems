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

type sVersion struct {
	val     string
	version rpc.Tversion
}

type KVServer struct {
	mu      sync.Mutex
	mapping map[string]sVersion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mapping: make(map[string]sVersion),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, exists := kv.mapping[args.Key]

	if exists {
		reply.Value = val.val
		reply.Version = val.version
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, exists := kv.mapping[args.Key]

	if exists {
		if args.Version != val.version {
			reply.Err = rpc.ErrNoKey
			return
		}
		val.val = args.Value
		val.version += 1
		kv.mapping[args.Key] = val

		return
	} else {
		if args.Version != rpc.Tversion(0) {
			reply.Err = rpc.ErrNoKey
			return
		}

		kv.mapping[args.Key] = sVersion{
			val:     args.Value,
			version: 1,
		}
		return
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
