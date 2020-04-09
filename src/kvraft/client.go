package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIdx int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	// TODO: find out the leader
	ck.leaderIdx = int(nrand() % int64(len(servers)))
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
	args := GetArgs{Key: key}
	reply := GetReply{}
	// TODO: now calling a random server, update later
	ok := ck.servers[ck.leaderIdx].Call("KVServer.Get", &args, &reply)
	if ok {
		i := ck.leaderIdx
		for ok && reply.Err == ErrWrongLeader {
			// try a different one
			i = int(nrand() % int64(len(ck.servers)))
			ok = ck.servers[i].Call("KVServer.Get", &args, &reply)
		}
		ck.leaderIdx = i
		if reply.Err == "" {
			return reply.Value
		}
	}

	return reply.Value // TODO: error handling
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
	DPrintf("Got putappend request key %s, value %s, op %s", key, value, op)
	args := PutAppendArgs{Key: key, Value: value, Op: op}
	reply := PutAppendReply{}
	// TODO: calling leader
	ok := ck.servers[ck.leaderIdx].Call("KVServer.PutAppend", &args, &reply)
	if ok {
		i := ck.leaderIdx
		for ok && reply.Err == ErrWrongLeader {
			i = int(nrand() % int64(len(ck.servers)))
			ok = ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		}
		ck.leaderIdx = i
		if reply.Err == "" {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
