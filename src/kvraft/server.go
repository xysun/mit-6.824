package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string // GET/PUT/APPEND
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// KV map
	store map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// return current `store`? or we should commit a Get first
	_, _, isLeader := kv.rf.Start(Op{Key: args.Key, Op: "Get"})
	if isLeader {

		// wait on applyCh; note you may not receive the msg you want?
		// what about timeouts?
		m := <-kv.applyCh             // will block until a msg is sent
		command, ok := m.Command.(Op) // TODO: handle ok is false
		if ok {
			if command.Op == "Get" && command.Key == args.Key {
				v, keyExists := kv.store[args.Key] // TODO: lock
				if !keyExists {
					reply.Err = ErrNoKey
				} else {
					reply.Value = v
				}
			} else {
				reply.Err = ErrWrongCommit
			}
		} else {
			reply.Err = "Failed to typecast Command to Op"
		}

	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// calling Start(), wait for applyCh, update store
	_, _, isLeader := kv.rf.Start(Op{Key: args.Key, Value: args.Value, Op: args.Op})
	if isLeader {
		// TODO: wait for applyCh
		m := <-kv.applyCh
		command, ok := m.Command.(Op)
		if ok {
			if command.Op != args.Op || command.Key != args.Key {
				reply.Err = "Commit with wrong op received"
			} else {
				// update store
				DPrintf("[kfraft][%d] Received command %s, updating store", kv.me, command)
				kv.store[args.Key] = args.Value // TODO: handle append
			}
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = make(map[string]string)

	// TODO: start listening on channel

	return kv
}
