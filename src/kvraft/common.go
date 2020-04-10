package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongCommit = "ErrWrongCommit"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Id  string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
