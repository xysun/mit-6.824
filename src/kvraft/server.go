package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

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
	Id    string
}

type AppliedOp struct {
	Id    string
	Value string
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
	store   map[string]string
	applied []AppliedOp
}

func (kv *KVServer) consumeCh() {
	for {
		m := <-kv.applyCh
		if m.CommandValid {
			kv.mu.Lock()
			command := m.Command.(Op)

			DPrintf("[kvraft][%d] Got commit message %+v", kv.me, m)
			// duplicate detection
			duplicated := false
			for i := 0; i < len(kv.applied); i++ {
				if kv.applied[i].Id == command.Id {
					DPrintf("[kvraft][%d] This commit %s is duplicated", kv.me, command.Id)
					duplicated = true
					break
				}
			}
			switch command.Op {
			case "Put":
				if !duplicated {
					kv.store[command.Key] = command.Value
					kv.applied = append(kv.applied, AppliedOp{Id: command.Id})
				}

			case "Append":
				if !duplicated {
					v, ok := kv.store[command.Key]
					if ok {
						kv.store[command.Key] = v + command.Value
					} else {
						kv.store[command.Key] = command.Value
					}
					kv.applied = append(kv.applied, AppliedOp{Id: command.Id})
				}

			case "Get":
				if !duplicated {
					v, ok := kv.store[command.Key]
					if ok {
						kv.applied = append(kv.applied, AppliedOp{Id: command.Id, Value: v})
					} else {
						kv.applied = append(kv.applied, AppliedOp{Id: command.Id, Value: ErrNoKey})
					}
				}

			}
			kv.mu.Unlock()
		}

	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// return current `store`? or we should commit a Get first
	_, _, isLeader := kv.rf.Start(Op{Key: args.Key, Op: "Get", Id: args.Id})
	if isLeader {
		committed := false
		i := 0
		for !committed {
			time.Sleep(20 * time.Millisecond) // TODO timeout
			kv.mu.Lock()

			for ; i < len(kv.applied); i++ {
				if kv.applied[i].Id == args.Id {
					committed = true
					if kv.applied[i].Value == ErrNoKey {
						reply.Err = ErrNoKey
					} else {
						reply.Value = kv.applied[i].Value
					}
					DPrintf("[kvraft][%d] This Get ops has been committed! %s, %s", kv.me, args.Id, reply.Value)
					break
				}
			}

			kv.mu.Unlock()
		}

	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// calling Start(), wait for applyCh, update store

	_, _, isLeader := kv.rf.Start(Op{Key: args.Key, Value: args.Value, Op: args.Op, Id: args.Id})
	if isLeader {
		DPrintf("[%d] Got PutAppend request %+v", kv.me, args)
		// TODO: check that we have received ApplyMsg
		committed := false
		i := 0
		for !committed {
			// keep checking kv.applied for the uuid
			time.Sleep(20 * time.Millisecond) // TODO: eventual timeout
			kv.mu.Lock()
			for ; i < len(kv.applied); i++ {
				if kv.applied[i].Id == args.Id {
					committed = true
					DPrintf("[kvraft][%d] This PutAppend ops has been committed! %s", kv.me, args.Id)
					break
				}
			}

			kv.mu.Unlock()
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

	// start listening on channel
	go kv.consumeCh()

	return kv
}
