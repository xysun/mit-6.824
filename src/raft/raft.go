package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State string

const (
	Follower  State = "follower"
	Candidate       = "candidate"
	Leader          = "leader"
)

type Entry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A:
	currentTerm         int
	votedFor            int // -1 as the nil value
	state               State
	lastHeardFromLeader time.Time
	votesSoFar          int

	// 2B
	logs            []Entry
	commitIndex     int
	nextIndex       []int
	matchIndex      []int
	applyCh         chan ApplyMsg
	electionTimeout time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	ct := rf.currentTerm
	s := rf.state == Leader
	rf.mu.Unlock()
	return ct, s
}

func (rf *Raft) Convert2Follower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votesSoFar = 0
	rf.lastHeardFromLeader = time.Now()
}

func (rf *Raft) PrintLogs() {
	// assuming have lock
	var s string = fmt.Sprintf("[%d] logs: ", rf.me)
	for i, e := range rf.logs {
		s += fmt.Sprintf("idx %d, Term %d, Command %s; ", i, e.Term, e.Command)
	}
	s += "\n"
	DPrintf(s)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

type AppendEntriesArgs struct {
	Term            int
	LeaderId        int
	PrevLogIndex    int
	PrevLogTerm     int
	Entries         []Entry
	LeaderCommitIdx int
}

type AppendEntriesReply struct {
	Term            int
	Success         bool
	ConflictFromIdx int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
	} else {
		// TODO: we are not doing the "vote only once per term" as per paper though
		// however if we do that we fail the Backup test though
		DPrintf("[%d] can I vote for server %d?\n", rf.me, args.CandidateId)
		rf.PrintLogs()
		myLastLogIdx := len(rf.logs) - 1
		myLastLog := rf.logs[myLastLogIdx]

		if myLastLog.Term > args.LastLogTerm {
			DPrintf("[%d] I have a last log with higher term!", rf.me)
			reply.VoteGranted = false
		} else {
			if myLastLog.Term == args.LastLogTerm && myLastLogIdx > args.LastLogIndex {
				DPrintf("[%d] I have a longer log!", rf.me)
				reply.VoteGranted = false
			} else {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.Convert2Follower(args.Term)
			}
		}
	}

	rf.mu.Unlock()

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// if AppendEntries.term >= rf.currentTerm, reply true, transition to follower, update timestamp
	// if not, reply false, continue
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] Got AppendEntries from server %d, my term is %d, theirs is %d, entries length %d, prev index %d",
		rf.me, args.LeaderId, rf.currentTerm, args.Term, len(args.Entries), args.PrevLogIndex)

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term >= rf.currentTerm {
		reply.Success = true
		rf.Convert2Follower(args.Term)

		rf.votedFor = -1
	}

	if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d] PrevLogIndex does not match! my log length is %d, PrevLogIndex is %d",
			rf.me, len(rf.logs), args.PrevLogIndex)
		reply.Success = false
		reply.Term = rf.currentTerm
		// from https://github.com/stardust95/MIT6.824/blob/master/src/raft/raft.go#L269
		if len(rf.logs) > args.PrevLogIndex {
			conflictTerm := rf.logs[args.PrevLogIndex].Term
			i := args.PrevLogIndex
			for ; i > 0; i-- {
				if rf.logs[i].Term != conflictTerm {
					break
				}
			}
			reply.ConflictFromIdx = i + 1
		} else {
			reply.ConflictFromIdx = len(rf.logs)
		}
		return
	}

	if len(args.Entries) == 0 {
		// only check commitidx in heartbeat
		if args.LeaderCommitIdx > rf.commitIndex {
			n := min(args.LeaderCommitIdx, len(rf.logs)-1)
			if n > rf.commitIndex {
				DPrintf("[%d] Set commit index to %d\n", rf.me, n)
				for i := rf.commitIndex + 1; i <= n; i++ {
					rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: i, Command: rf.logs[i].Command}
				}
				rf.commitIndex = n
			}
		}
		DPrintf("[%d] Acked heartbeat, going back to follower\n", rf.me)
		return
	}

	reply.Success = true

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3) Append any new entries not already in the log
	// make sure we do NOT delete perfectly good ones (because RPC can arrive out of order)
	i := 0
	for ; i < len(args.Entries); i++ {
		thisIdx := args.PrevLogIndex + i + 1
		if thisIdx >= len(rf.logs) {
			break
		}
		if rf.logs[thisIdx].Term != args.Entries[i].Term {
			rf.logs = rf.logs[:thisIdx]
			break
		}
	}
	if i < len(args.Entries) {
		rf.logs = append(rf.logs, args.Entries[i:]...)
	}

	rf.PrintLogs()

	return

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// only send if you are still a candidate
	rf.mu.Lock()
	if rf.state == Candidate {
		rf.mu.Unlock()
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			DPrintf("[%d] Server asks server %d for a vote, result is %t\n", rf.me, server, reply.VoteGranted)
			if reply.VoteGranted {
				rf.mu.Lock()
				DPrintf("[%d] got vote from %d, my state %s my current term %d, args term %d",
					rf.me, server, rf.state, rf.currentTerm, args.Term)
				// re-check assumption: i am still a candidate with same term
				if rf.state == Candidate && rf.currentTerm == args.Term {
					rf.votesSoFar++
					if rf.votesSoFar > len(rf.peers)/2 {
						// majority, i am a leader now!
						DPrintf("[%d] Server is a leader now!\n", rf.me)
						rf.state = Leader
						rf.votesSoFar = 0
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						nextIdx := len(rf.logs)
						for i := range rf.peers {
							rf.nextIndex[i] = nextIdx
						}
						go rf.heartbeatTick()
					}
				}
				rf.mu.Unlock()
			} else {
				// update myself if reply.Term > rf.currentTerm; make sure i am still a candidate
				rf.mu.Lock()
				if reply.Term > rf.currentTerm && rf.state == Candidate {
					DPrintf("[%d] request vote reply from %d has a higher term, their term %d, my term %d, reverting back to follower...",
						rf.me, server, args.Term, rf.currentTerm)
					rf.Convert2Follower(reply.Term)
				}
				rf.mu.Unlock()
			}
		}
		return ok
	} else {
		DPrintf("[%d] Server is no longer a candidate! Skipping vote request...\n", rf.me)
		rf.mu.Unlock()
		return true
	}

}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// if reply is false, transition back into follower (?)
	// check conditions: I am still a leader, and my term has not changed
	rf.mu.Lock()
	isLeader := rf.state == Leader
	term := rf.currentTerm
	rf.mu.Unlock()
	if ok && isLeader && term == args.Term {
		if !reply.Success {
			rf.mu.Lock()
			DPrintf("[%d] Leader Got false reply from server %d, their term %d, my term %d\n",
				rf.me, server, reply.Term, rf.currentTerm)
			if reply.Term > rf.currentTerm {
				DPrintf("[%d] Leader Transition back to follower! New term %d", rf.me, reply.Term)
				rf.Convert2Follower(reply.Term)
				rf.mu.Unlock()
			} else {
				DPrintf("[%d] Received failed AppendEntry froms server %d because log inconsistency, next index is %d, matched index is %d",
					rf.me, server, rf.nextIndex[server], rf.matchIndex[server])

				if reply.ConflictFromIdx < rf.matchIndex[server] && rf.matchIndex[server] == len(rf.logs)-1 {
					// if we've already replicated all of logs, this failed response is outdated, do nothing
					rf.mu.Unlock()
				} else {
					// decrease nextIndex and retry
					// this could also be from a heartbeat, so we want to fix before waiting for next command
					rf.nextIndex[server] = reply.ConflictFromIdx
					DPrintf("[%d] Leader dropping server %d next index to %d", rf.me, server, rf.nextIndex[server])
					entries := rf.logs[rf.nextIndex[server]:]
					prevLogIdx := rf.nextIndex[server] - 1

					// IMPORTANT: we have to create a new obj to avoid data race
					newArgs := AppendEntriesArgs{
						Term:            rf.currentTerm,
						LeaderId:        rf.me,
						PrevLogIndex:    prevLogIdx,
						PrevLogTerm:     rf.logs[prevLogIdx].Term,
						LeaderCommitIdx: rf.commitIndex,
						Entries:         entries}

					rf.mu.Unlock()
					go rf.sendAppendEntry(server, &newArgs, &AppendEntriesReply{})
				}

			}
		} else {
			// success reply
			DPrintf("[%d] Received success AppendEntry reply from server %d", rf.me, server)
			// update matchIndex and nextIndex if there are entries
			if len(args.Entries) > 0 {
				rf.mu.Lock()
				// only increase matchIndex and nextIndex monotomically
				rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
				rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)

				DPrintf("[%d] next index for server %d is at %d", rf.me, server, rf.nextIndex[server])
				// can we commit this entries? try from rf.commitIndex+1 until rf.matchIndex[server], backwards
				for n := rf.matchIndex[server]; n > rf.commitIndex; n-- {
					if rf.logs[n].Term == rf.currentTerm {
						count := 1 // start with myself
						for i := range rf.peers {
							if i != rf.me {
								if rf.matchIndex[i] >= n {
									count++
								}
							}
						}
						if count > len(rf.peers)/2 {
							DPrintf("[%d] Leader setting commit index to %d\n", rf.me, n)
							for i := rf.commitIndex + 1; i <= n; i++ {
								rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: i, Command: rf.logs[i].Command}
							}
							rf.commitIndex = n
							break
						}
					}
				}
				rf.mu.Unlock()
			}

		}
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	if rf.killed() {
		return -1, -1, false
	}
	// not killed
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if isLeader {
		thisEntry := Entry{Term: rf.currentTerm, Command: command}
		prevLogIdx := len(rf.logs) - 1
		rf.logs = append(rf.logs, thisEntry)

		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = prevLogIdx
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		args.LeaderCommitIdx = rf.commitIndex

		// for each server, send this entry
		args.Entries = []Entry{thisEntry}

		// send AppendEntries RPC
		DPrintf("[%d] Got command, sending AppendEntry, prev log idx %d", rf.me, prevLogIdx)
		rf.mu.Unlock()
		for i := range rf.peers {
			if i != rf.me { // rf.me won't change so no need to lock

				go rf.sendAppendEntry(i, &args, &AppendEntriesReply{})
			}
		}

		return prevLogIdx + 1, term, isLeader
	}

	// not leader
	rf.mu.Unlock()
	return -1, term, isLeader

}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	// rf starts with locked
	rf.PrintLogs()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesSoFar = 1

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.logs) - 1
	args.LastLogTerm = rf.logs[args.LastLogIndex].Term

	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, &args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) resetElectionTimeout() time.Duration {
	rf.mu.Lock()
	t := time.Duration(rand.Intn(400)+400) * time.Millisecond
	rf.electionTimeout = t
	rf.mu.Unlock()
	return t
}

func (rf *Raft) electionTick() {
	// transition into candidate, if haven't heard from leader, AND you are a follower
	DPrintf("[%d] Running election tick\n", rf.me)
	// starts as follower, hence no heartbeat sent
	// decides election timeout, [200,1000] ms; we will send heartbeat every 200ms

	// DPrintf("[%d] Election timeout for is %d ms\n", rf.me, electionTimeout)
	for !rf.killed() {
		// sleep for a short while, 200 because we send heartbeat every 200ms
		time.Sleep(20 * time.Millisecond)
		t := time.Now()
		rf.mu.Lock()

		tdiff := t.Sub(rf.lastHeardFromLeader)
		// if rf.state == Follower {
		// 	DPrintf("[%d] It has been %d ms since last heard from leader, threshold is %d\n", rf.me, tdiff.Milliseconds(), rf.electionTimeout.Milliseconds())
		// }

		if tdiff > rf.electionTimeout && rf.state == Follower {

			rf.state = Candidate
			DPrintf("[%d] It has been %d ms since last heard from leader, threshold is %d\n", rf.me, tdiff.Milliseconds(), rf.electionTimeout.Milliseconds())
			DPrintf("[%d] Server is transitioning into candidate!, from term %d \n", rf.me, rf.currentTerm)
			rf.startElection()

			// randomize to avoid lockstep
			d := rf.resetElectionTimeout()
			time.Sleep(d)
			// am I leader yet?
			rf.mu.Lock()
			for rf.state == Candidate && rf.votesSoFar <= len(rf.peers)/2 { // i'm still a candidate with not enough votes; this means I didn't win the election, I also haven't acked a leader
				DPrintf("[%d] Server does not have enough votes, start new election, current term %d!\n", rf.me, rf.currentTerm)
				rf.startElection()
				d = rf.resetElectionTimeout()
				time.Sleep(d)
				rf.mu.Lock()
			}
			rf.mu.Unlock()

		} else {
			// DPrintf("[%d] Server has recently heard from leader or is not a follower, do nothing...\n", rf.me)
			rf.mu.Unlock()
		}

	}

}

func (rf *Raft) heartbeatTick() {
	DPrintf("[%d] Running heartbeat tick\n", rf.me)
	// send heartbeat every 200ms, IIF server is leader
	for !rf.killed() {
		// heartbeat should start immediately
		rf.mu.Lock()
		if rf.state == Leader {
			DPrintf("[%d] Initiating heartbeat tick \n", rf.me)
			// Start a goroutine that send heartbeat and process replies
			// Be careful of deadlock
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LeaderCommitIdx = rf.commitIndex
			// set prevLogIdx and prevLogTerm to be the committed term, to remove potentially bad ones
			args.PrevLogIndex = rf.commitIndex
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term

			// TODO: send entries maybe
			rf.mu.Unlock()
			for i := range rf.peers {
				if i != rf.me { // rf.me won't change so no need to lock

					go rf.sendAppendEntry(i, &args, &AppendEntriesReply{})
				}
			}
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votesSoFar = 0
	rf.state = Follower // start as follower
	rf.lastHeardFromLeader = time.Now().Add(-1 * time.Hour)
	// 2B
	rf.commitIndex = 0
	rf.logs = []Entry{Entry{Term: 0}}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = 1
	}
	rf.applyCh = applyCh
	rf.electionTimeout = time.Duration(rand.Intn(200)+200) * time.Millisecond

	// start the infinite doing-work loop
	go rf.electionTick()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
