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

import "sync"
import "sync/atomic"
import "time"
import "../labrpc"
import "math/rand"

// import "bytes"
// import "../labgob"

const (
	Leader int	= 0
	Follower 	= 1
	Candidate	= 2
)

const (
	ElectionLowerBound int = 1000
	ElectionUpperBound int = 2000
)

const HeartBeatInterval int = 150

const CommonInterval int = 10

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

type Log struct {
	command		interface{}
	term		int
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
	votedFor   			int
	currentTerm			int
	log					[]Log

	// for election
	leaderId   			int
	currentState		int
	election_timer   	time.Time
	heartbeat_timer		time.Time

	// for log
	commitIndex			int
	lastApplied			int
	nextIndex			[]int
	matchIndex			[]int

	// for commit
	channel				chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.currentState == Leader
	
	return term, isleader
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
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]interface{}
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term	int
	Success bool
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// reset the timer
	if rf.currentTerm > args.Term {
		// return false
		reply.Term = rf.currentTerm
		reply.Success = false
	} else  {
		// heart beat packet
		rf.election_timer = time.Now()
		// DPrintf("[%d] receive AppendEntries from %d term %d currTerm %d prevLogIndex %d prevLogTerm %d", rf.me, args.LeaderId, args.Term, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm)
		rf.leaderId = args.LeaderId
		rf.currentTerm = args.Term
		// if rf.currentState != Follower {
		// 	DPrintf("[%d] changed currentState from %d to follower --- Arg %v", rf.me, rf.currentState, args)
		// }
		rf.currentState = Follower

		reply.Term = rf.currentTerm

		// if it's the first log, accept anyway
		if args.PrevLogIndex + 1 > len(rf.log) || rf.log[args.PrevLogIndex].term != args.PrevLogTerm {
			reply.Success = false
			return
		}
		
		if len(rf.log) > args.PrevLogIndex + 1 && rf.log[args.PrevLogIndex + 1].term != args.Term {
			// remove the conflict log
			DPrintf("[%d] truncate the log", rf.me)
			rf.log = rf.log[:args.PrevLogIndex + 1]
		}
		
		if len(args.Entries) > 0 {
			if len(rf.log) <= args.PrevLogIndex + 1 {
				rf.log = append(rf.log, Log{ command: args.Entries[0], term: args.Term })
			}
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
		}
		if len(args.Entries) > 0 {
			DPrintf("[%d] Successfully append the entry term=%d curLogLength=%d commitIndex=%d", rf.me, rf.currentTerm, len(rf.log), rf.commitIndex)
		}
		reply.Success = true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term            int
	CandidateId	    int
	LastLogIndex    int
    LastLogTerm     int
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
	defer rf.mu.Unlock()
	DPrintf("[%d] get RequestVote RPC from %d currentState=%v term=%v", rf.me, args.CandidateId, rf.currentState, args.Term)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	
	if args.Term > rf.currentTerm || rf.votedFor == -1 {
		// If RPC request contains term T > currentTerm, set currentTerm = T, convert to follower
		rf.currentTerm = args.Term
		rf.currentState = Follower

		if len(rf.log) > 1 {
			// when follower have the log and the candidate didn't
			if args.LastLogTerm == 0 {
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
				return
			}

			curTerm := rf.log[len(rf.log) - 1].term
			// when follower has the newer log
			if curTerm > args.LastLogTerm {
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
				return
			}

			// when follower has the newer log
			if curTerm == args.LastLogTerm && len(rf.log) > args.LastLogIndex + 1 {
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
				return
			}

			// otherwise, grant the vote
			// fall through
		}

		// update timer only when we grant vote
		rf.election_timer = time.Now()

		// if we don't have the log, grant anyway
		DPrintf("[%d] vote for %d", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		return
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) callRequestVote(server int, term int) bool {
	rf.mu.Lock()
	args := RequestVoteArgs{}
	args.Term = term
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[args.LastLogIndex].term
	rf.mu.Unlock()

	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)

	if !ok || !reply.VoteGranted || reply.Term > term {
		return false
	}

	return true
}

func (rf *Raft) startNewElection() {
	rf.mu.Lock()
	
	// increase term number
	rf.currentTerm += 1
	// vote for self
	rf.votedFor = rf.me
	rf.currentState = Candidate

	counter := 1

	term := rf.currentTerm
	done := false

	rf.mu.Unlock()

	DPrintf("[%d] start to Election term %d", rf.me, term)

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}

		go func(idx int) {
			ok := rf.callRequestVote(idx, term)

			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			counter++
			DPrintf("[%d] get voted by %d current count %d", rf.me, idx, counter)

			if done || counter < rf.getMajority() {
				return
			}

			done = true
			if term == rf.currentTerm {
				rf.currentState = Leader

				go rf.sendAllHeartBeatPackage(rf.currentTerm)
				rf.heartbeat_timer = time.Now()

				DPrintf("[%d] Wins to be a leader at term %d", rf.me, term)
				// reinitialized the leader state
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
			}
		}(idx)
	}
}

func (rf *Raft) getMajority() int {
	// since we won't change our cluster members, thus we don't need to acquire lock here
	return len(rf.peers) / 2 + 1
}

func (rf *Raft) singleAppendEntries(term, index, server int) {
	rf.mu.Lock()
	args := AppendEntriesArgs{}
	args.Term = term
	args.LeaderId = rf.me
	args.PrevLogIndex = index - 1
	args.PrevLogTerm = rf.log[index - 1].term
	args.Entries = make([]interface{}, 1)
	args.Entries[0] = rf.log[index].command
	args.LeaderCommit = rf.commitIndex
	// DPrintf("[%d] AppendEntries to %d term=%d prevLogIndex=%d prevLogTerm=%d commitIndex=%d", rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return
	}


	if reply.Term > term {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.leaderId = -1
			rf.currentState = Follower
		}
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success {
		rf.matchIndex[server] = index
		rf.nextIndex[server] = index + 1
		if rf.currentTerm == term && rf.nextIndex[server] < len(rf.log) {
			go rf.singleAppendEntries(term, rf.nextIndex[server], server)
		}
	} else {
		rf.nextIndex[server] = index - 1
		if rf.currentTerm == term {
			go rf.singleAppendEntries(term, rf.nextIndex[server], server)
		}
	}
}

func (rf *Raft) startAppendEntries(term int, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.singleAppendEntries(term, rf.nextIndex[idx], idx)
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState != Leader {
		isLeader = false
		return index, term, isLeader
	}

	term = rf.currentTerm
	index = len(rf.log)
	new_entry := Log{ command: command, term: term }
	rf.log = append(rf.log, new_entry)

	go rf.startAppendEntries(term, index)

	return index, term, isLeader
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

func (rf *Raft) electionThread() {
	for atomic.LoadInt32(&rf.dead) != 1 {
		interval := time.Duration(rand.Intn(ElectionUpperBound - ElectionLowerBound) + ElectionLowerBound)
		time.Sleep(time.Millisecond * interval)
		rf.mu.Lock()
		if time.Now().Sub(rf.election_timer) > time.Millisecond * interval && rf.currentState != Leader {
			// start a new election
			go rf.startNewElection()
		}
		rf.election_timer = time.Now()
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartBeatPackage(server int, term int) {
	rf.mu.Lock()
	args := AppendEntriesArgs{}
	args.Term = term
	args.LeaderId = rf.me
	index := rf.nextIndex[server]
	args.PrevLogIndex = index - 1
	args.PrevLogTerm = rf.log[index - 1].term
	args.LeaderCommit = rf.commitIndex
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > term && reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.currentState = Follower
	}

	if !reply.Success {
		if rf.nextIndex[server] == index {
			rf.nextIndex[server] = index - 1
		}
	}
}

func (rf *Raft) sendAllHeartBeatPackage(term int) {
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}

		go rf.sendHeartBeatPackage(idx, term)
	}
}

func (rf *Raft) heartbeatThread() {
	for atomic.LoadInt32(&rf.dead) != 1 {
		// DO NOT EVER MULTIPLY DURATION WITH DURATION
		interval := time.Duration(HeartBeatInterval)
		time.Sleep(time.Millisecond * interval)
		rf.mu.Lock()
		if time.Now().Sub(rf.heartbeat_timer) > time.Millisecond * interval && rf.currentState == Leader {
			go rf.sendAllHeartBeatPackage(rf.currentTerm)
			rf.heartbeat_timer = time.Now()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) commitThread() {
	for atomic.LoadInt32(&rf.dead) != 1 {
		interval := time.Duration(CommonInterval)
		time.Sleep(time.Millisecond * interval)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			index := rf.lastApplied
			msg := ApplyMsg{ CommandIndex: index, CommandValid: true, Command: rf.log[index].command }
			DPrintf("[%d] commit index=%d curTerm=%d", rf.me, index, rf.currentTerm)
			rf.mu.Unlock()
			rf.channel <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitIndexThread() {
	for atomic.LoadInt32(&rf.dead) != 1 {
		interval := time.Duration(CommonInterval)
		time.Sleep(time.Millisecond * interval)
		rf.mu.Lock()
		if rf.currentState == Leader {
			N := rf.commitIndex + 1
			shouldExit := false
			for !shouldExit {
				counter := 1
				for idx := range rf.peers {
					if rf.matchIndex[idx] >= N {
						counter++
					}
				}
				if counter >= rf.getMajority() {
					if rf.log[N].term == rf.currentTerm {
						rf.commitIndex = N
						DPrintf("[%d] update commit index %d curTerm=%d", rf.me, rf.commitIndex, rf.currentTerm)
					}
					N++
				} else {
					shouldExit = true
				}
				if N >= len(rf.log) {
					shouldExit = true
				}
			}
		}
		rf.mu.Unlock()
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

	rf.votedFor = -1
	rf.currentTerm = 0
	rf.leaderId = -1
	rf.currentState = Follower
	rf.election_timer = time.Now()
	rf.heartbeat_timer = time.Now()

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]Log, 1)
	rf.log[0] = Log{ term: 0 }

	rf.channel = applyCh

	go rf.heartbeatThread()
	go rf.electionThread()
	go rf.commitThread()
	go rf.updateCommitIndexThread()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
