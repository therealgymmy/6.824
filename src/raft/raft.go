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
	//	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	timeoutCond   *sync.Cond // Conditional variable for coordinating elections
	timeoutIndex  int        // Monotically increasing state to coordinate elctions
	timeoutResult bool       // If true, an election should occur

	state       RaftState // Different state leads to a differet set of rules to follow
	currentTerm int       // Latest term we have seen
	votedFor    int       // Candidate we have voted for. -1 means nil.

	debugLogEnabled bool // If true, print debug logs
}

func (rf *Raft) dlog(format string, a ...interface{}) {
	enabled := rf.debugLogEnabled
	if enabled {
		log.Printf(format, a...)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	rf.mu.Unlock()

	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// If current term is older than sender, we will revert to follower.
// Return true to indicate we're reverted to follower
func (rf *Raft) mayRevertToFollowerLocked(senderCurrentTerm int) bool {
	// If current term is older than sender, we will revert to follower.
	termIsOlder := rf.currentTerm < senderCurrentTerm
	if !termIsOlder {
		return false
	}

	rf.currentTerm = senderCurrentTerm
	rf.votedFor = -1
	rf.state = Follower
	return true
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // Candidate's current term
	CandidateId int // ID of Candidate requesting vote
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // Receiver's current term. Candidate need to update itself if this is newer. -1 means nil.
	VoteGranted bool // True means candidate received vote.
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Check for common rules.
	reverted := rf.mayRevertToFollowerLocked(args.Term)
	if reverted {
		rf.dlog("[%d]: info: RequestVote: reverted to follower. Sender: %d", rf.me, args.CandidateId)
	}

	// 2. Withhold vote if sender's term is less than our term.
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.dlog(
			"[%d]: info: RequestVote: withheld vote because our term is newer. Sender: %d, Our Term: %d, Sender Term: %d",
			rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	// 3. Grant vote if votedFor is null or candidateId, and candidate's log is at least
	//    as up-to-date as our log.
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = -1
		reply.VoteGranted = false
		rf.dlog(
			"[%d]: info: RequestVote: withheld vote because we already voted. Sender: %d, Our Chosen Candidate: %d",
			rf.me, args.CandidateId, rf.votedFor)
		return
	}

	// TODO
	// 4. Check sender's log is at least as up-to-date as ours

	// 5. Set ourselves to follower and acknowledge the sender as our leader.
	rf.votedFor = args.CandidateId
	rf.state = Follower

	reply.Term = -1
	reply.VoteGranted = true

	// Reset timer since we have granted a vote to the sender.
	rf.timeoutCond.Signal()

	rf.dlog(
		"[%d]: info: RequestVote: granted vote. Sender: %d, Term: %d",
		rf.me, args.CandidateId, rf.currentTerm)
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int // Leader's current term
	LeaderId int // ID of the leader
}

type AppendEntriesReply struct {
	Term    int  // Receiver's current term. Leader needs to update itself if this is newer. -1 means nil.
	Success bool // True if receiver contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Check for common rules.
	reverted := rf.mayRevertToFollowerLocked(args.Term)
	if reverted {
		rf.dlog("[%d]: info: AppendEntries: reverted to follower. Sender: %d", rf.me, args.LeaderId)
	}

	// 2. Reply false if sender's term is less than our term.
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// TODO
	// 3. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.
	// 4. If an existing entry conflicts with a new one, delete the existing entry and all that follow it.
	// 5. Append any new entries not already in the log.
	// 6. If leaderCommit is larger than commitIndex, set commitIndex = min(leaderCommit, index of last new entry).

	// Reset timer since we have successfully processed a heartbeat.
	rf.timeoutCond.Signal()

	rf.dlog("[%d]: info: AppendEntries: processed successfully. Sender: %d", rf.me, args.LeaderId)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	resetToFollowerLocked := func() {
		rf.state = Follower
		rf.votedFor = -1
	}

	// 1. Prepare for election.
	//   - Increment our current term.
	//   - Vote for ourselves.
	//   - Construct RequestVoteArgs.
	//   - Reset election timer.
	rf.mu.Lock()
	incomingIndex := rf.timeoutIndex
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	numPeers := len(rf.peers)
	args := RequestVoteArgs{rf.currentTerm, rf.me}
	me := rf.me

	rf.dlog("[%d]: info: Election: starting. Term: %d, Index: %d", rf.me, rf.currentTerm, incomingIndex)
	rf.mu.Unlock()

	// 2. Send RequestVote RPCs to all other servers.
	ch := make(chan RequestVoteReply)
	for i := 0; i < numPeers; i++ {
		// We do not need to request vote from ourselves.
		if me == i {
			continue
		}

		go func(peerId int) {
			rf.dlog("[%d]: info: Election: requesting for vote. Peer: %d", me, peerId)

			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peerId, &args, &reply)

			// Send the vote result to be processed.
			ch <- reply

			if !ok {
				rf.dlog("[%d]: error: Election: request failed. Peer: %d", me, peerId)
				return
			}

			rf.mu.Lock()

			outgoingIndex := rf.timeoutIndex
			// If index mismatch, the current timer has already reset. This means we'll
			// simply ignore the result of the elections.
			if incomingIndex != outgoingIndex {
				rf.dlog(
					"[%d]: info: Election: timed out. Peer: %d, Old Index: %d, New Index: %d",
					me, peerId, incomingIndex, outgoingIndex)
				rf.mu.Unlock()
				return
			}

			// If we have found a higher term, we revert back to being a follower.
			if reply.Term > args.Term {
				rf.dlog(
					"[%d]: info: Election: reply contains higher term. Peer: %d, Our Term: %d, Peer Term: %d",
					me, peerId, args.Term, reply.Term)

				resetToFollowerLocked()

				rf.currentTerm = args.Term

				// We also need to reset the election timer
				rf.timeoutResult = false
				rf.mu.Unlock()
				rf.timeoutCond.Signal()
				return
			}

			rf.mu.Unlock()
		}(i)
	}

	// 3. Check results and determine outcome of the election.
	// We start with one initial vote since we have already voted for ourselves.
	votes := 1
	for i := 1; i < numPeers; i++ {
		reply := <-ch
		if reply.VoteGranted {
			votes++
		}
		if votes > numPeers/2 {
			rf.dlog("[%d]: info: Election: stopping the count with %d votes.", me, votes)
			break
		}
	}

	rf.mu.Lock()
	outgoingIndex := rf.timeoutIndex

	// Fail election and revert to follower if
	// 	(a). Index mismatch: the current election has already expired.
	// 	(b). We failed to secure the majority of the votes.
	if incomingIndex != outgoingIndex ||
		votes <= numPeers/2 {
		rf.dlog(
			"[%d]: info: Election: failed with %d votes. Old Index: %d, New Index: %d",
			votes, me, incomingIndex, outgoingIndex)

		resetToFollowerLocked()
		rf.mu.Unlock()
		return
	}

	rf.dlog("[%d]: info: Election: succeeded with %d votes.", me, votes)

	// We have secured majority of the votes, we'll become leader now and send out heartbeats
	rf.state = Leader
	// We also need to reset the election timer
	rf.timeoutResult = false
	rf.mu.Unlock()
	rf.timeoutCond.Signal()
	go rf.sendHeartbeats()
}

func (rf *Raft) sendHeartbeats() {
	resetToFollowerLocked := func() {
		rf.state = Follower
		rf.votedFor = -1
	}

	rf.mu.Lock()
	incomingIndex := rf.timeoutIndex
	numPeers := len(rf.peers)
	args := AppendEntriesArgs{rf.currentTerm, rf.me}
	me := rf.me
	rf.mu.Unlock()

	ch := make(chan AppendEntriesReply)
	for i := 0; i < numPeers; i++ {
		// We do not need to send heartbeat to ourselves.
		if me == i {
			continue
		}

		go func(peerId int) {
			rf.dlog("[%d]: info: Heartbeat: sending. Peer: %d", me, peerId)

			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(peerId, &args, &reply)

			// Send the heartbeat result to be processed.
			ch <- reply

			if !ok {
				rf.dlog("[%d]: error: Heartbeat: request failed. Peer: %d", me, peerId)
			}

			rf.mu.Lock()

			outgoingIndex := rf.timeoutIndex
			// If index mismatch, the current timer has already reset. This means we'll
			// simply ignore the result of these heartbeat replies.
			if incomingIndex != outgoingIndex {
				rf.dlog(
					"[%d]: info: Heartbeat: timed out. Peer: %d, Old Index: %d, New Index: %d",
					me, peerId, incomingIndex, outgoingIndex)
				rf.mu.Unlock()
				return
			}

			// If we have found a higher term, we revert back to being a follower.
			if reply.Term > args.Term {
				rf.dlog(
					"[%d]: info: Heartbeat: reply contains higher term. Peer: %d, Our Term: %d, Peer Term: %d",
					me, peerId, args.Term, reply.Term)

				resetToFollowerLocked()

				rf.currentTerm = args.Term

				// We also need to reset the timer
				rf.timeoutResult = false
				rf.mu.Unlock()
				rf.timeoutCond.Signal()
				return
			}

			// Heartbeat was a success. Nothing else is needed.
			if reply.Success {
				rf.mu.Unlock()
				return
			}

			// TODO: log adjustment if heartbeat failed.
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) timeoutHandler() {
	rf.mu.Lock()
	isLeader := rf.state == Leader
	incomingIndex := rf.timeoutIndex
	rf.mu.Unlock()

	var sleepDuration int
	if isLeader {
		sleepDuration = 200 // heartbeat timeout
	} else {
		sleepDuration = 400 + rand.Intn(400) // election timeout
	}

	time.Sleep(time.Duration(sleepDuration) * time.Millisecond)

	rf.mu.Lock()
	outgoingIndex := rf.timeoutIndex

	// Timer can be restarted when elections fail or heartbeats downgrade us back to follower. If
	// the timer has been restarted, then this logic no longer needs to wake up the stateHandler
	// goroutine. This is because the stateHandler was already woken up by someone else and has
	// kicked off a new timer already.
	if incomingIndex != outgoingIndex {
		rf.mu.Unlock()
		return
	}
	rf.timeoutResult = true
	rf.mu.Unlock()
	rf.timeoutCond.Signal()
}

func (rf *Raft) stateHandler() {
	// Make sure you reset your election timer exactly when Figure 2 says you should. Specifically,
	// you should only restart your election timer if a) you get an AppendEntries RPC from the
	// current leader (i.e., if the term in the AppendEntries arguments is outdated, you should not
	// reset your timer); b) you are starting an election; or c) you grant a vote to another peer.
	for !rf.killed() {
		rf.mu.Lock()

		rf.timeoutIndex++
		rf.timeoutResult = false

		rf.dlog("[%d]: info: Timer: starting a new one. Index: %d", rf.me, rf.timeoutIndex)

		go rf.timeoutHandler()

		rf.timeoutCond.Wait()
		if rf.timeoutResult {
			rf.dlog("[%d]: info: Timer: fired. Index: %d, State: %d", rf.me, rf.timeoutIndex, rf.state)
			if rf.state == Follower || rf.state == Candidate {
				rf.mu.Unlock()
				go rf.startElection()
			} else {
				rf.mu.Unlock()
				go rf.sendHeartbeats()
			}
		} else {
			rf.dlog("[%d], info: Timer: reset. Index: %d", rf.me, rf.timeoutIndex)
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.timeoutCond = sync.NewCond(&rf.mu)
	rf.timeoutIndex = 0
	rf.timeoutResult = false

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	// Debug log enabled?
	rf.debugLogEnabled = true

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections and heartbeats
	go rf.stateHandler()

	return rf
}
