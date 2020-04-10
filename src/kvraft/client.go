package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
	"github.com/google/uuid"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIdx int
	id        string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// NOTE: servers are randomized!
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	// init with a random leader
	ck.leaderIdx = int(nrand() % int64(len(servers)))
	ck.id = uuid.New().String()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	DPrintf("[client %s] Got Get request key %s", ck.id, key)
	args := GetArgs{Key: key, Id: uuid.New().String()}
	reply := GetReply{}
	// now calling a random server, update later
	i := ck.leaderIdx
	ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
	for !ok || reply.Err != "" {
		if !ok {
			DPrintf("[client %s] RPC failed for Get %+v to server %d", ck.id, args, i)
		}
		if reply.Err != "" && reply.Err != ErrWrongLeader {
			DPrintf("[client %s] Get %+v to server %d has error %+v", ck.id, args, i, reply)
		}
		i = int(nrand() % int64(len(ck.servers)))
		reply = GetReply{}
		ok = ck.servers[i].Call("KVServer.Get", &args, &reply)
	}
	ck.leaderIdx = i
	DPrintf("[client %s] Success Get reply for args %+v from server %d is %+v, ok %t", ck.id, args, ck.leaderIdx, reply, ok)
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("[client %s] Got putappend request key %s, value %s, op %s", ck.id, key, value, op)
	args := PutAppendArgs{Key: key, Value: value, Op: op, Id: uuid.New().String()}
	reply := PutAppendReply{}
	i := ck.leaderIdx
	ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

	for !ok || reply.Err != "" {
		if !ok {
			DPrintf("[client %s] RPC failed for PutAppend %+v to server %d", ck.id, args, i)
		}
		if reply.Err != "" && reply.Err != ErrWrongLeader {
			DPrintf("[client %s] PutAppend %+v to server %d has error %+v", ck.id, args, i, reply)
		}
		i = int(nrand() % int64(len(ck.servers)))
		reply = PutAppendReply{}
		DPrintf("[client %s] Sending PutAppend %+v to server %d", ck.id, args, i)
		ok = ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
	}
	ck.leaderIdx = i
	DPrintf("[client %s] Success PutAppend reply for args %+v from server %d is %+v, ok %t", ck.id, args, ck.leaderIdx, reply, ok)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
