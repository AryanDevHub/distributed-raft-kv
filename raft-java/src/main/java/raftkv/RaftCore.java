package raftkv;

import raftkv.Messages.AppendEntriesRequest;
import raftkv.Messages.AppendEntriesResponse;
import raftkv.Messages.RequestVoteRequest;
import raftkv.Messages.RequestVoteResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Pure Raft state machine. It performs no I/O, owns no threads or timers and is NOT thread-safe:
 * the runtime serializes every call. Each event handler mutates in-memory state and returns the
 * {@link Action}s the runtime must execute, in order. If the hard state (term/vote) changed, the first
 * action is always {@link Action.PersistHardState}, so it is durable before any reply or request leaves.
 */
public final class RaftCore {

    public enum State { FOLLOWER, PRE_CANDIDATE, CANDIDATE, LEADER }

    /** A handler result: the reply to return plus the side effects to execute before returning it. */
    public record Outcome<T>(T value, List<Action> actions) {
    }

    public record ProposeResult(boolean accepted, long index, long term) {
    }

    private static final int MAX_ENTRIES_PER_RPC = 128;

    private final String selfId;
    private final List<String> peerIds;
    private final int quorum;

    private State state = State.FOLLOWER;
    private long currentTerm;
    private String votedFor;
    private String leaderId;

    /** log.get(i) holds the entry with index i + 1. */
    private final List<LogEntry> log;
    private long commitIndex;
    private long lastApplied;

    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Set<String> votes = new HashSet<>();
    private final Set<String> preVotes = new HashSet<>();
    /** Peers that answered this leader since its last check-quorum tick. */
    private final Set<String> heardFrom = new HashSet<>();
    private final boolean leaderStickiness;
    /** True if a legitimate leader contacted us since our election timer last fired. */
    private boolean heardLeader;
    private boolean hardStateDirty;

    /** Same as the full constructor with leader stickiness off. */
    public RaftCore(String selfId, Collection<String> peerIds, long currentTerm, String votedFor,
                    List<LogEntry> recoveredLog) {
        this(selfId, peerIds, currentTerm, votedFor, recoveredLog, false);
    }

    /**
     * @param peerIds          the other members of the cluster (excluding this node)
     * @param leaderStickiness if true, a node that has heard from a live leader since its election timer last
     *                         fired refuses to grant pre-votes. That stops a briefly disconnected node from
     *                         deposing a healthy leader, at the cost of a slower failover.
     */
    public RaftCore(String selfId, Collection<String> peerIds, long currentTerm, String votedFor,
                    List<LogEntry> recoveredLog, boolean leaderStickiness) {
        this.selfId = Objects.requireNonNull(selfId, "selfId");
        this.leaderStickiness = leaderStickiness;
        this.peerIds = List.copyOf(peerIds);
        // Strict majority of the full cluster (peers + self).
        this.quorum = ((this.peerIds.size() + 1) / 2) + 1;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log = new ArrayList<>(recoveredLog);
        for (int i = 0; i < log.size(); i++) {
            if (log.get(i).index() != i + 1L) {
                throw new IllegalArgumentException("recovered log is not contiguous at position " + i);
            }
        }
    }

    // ------------------------------------------------------------------ accessors

    public State state() { return state; }

    public long currentTerm() { return currentTerm; }

    public String leaderId() { return leaderId; }

    public String votedFor() { return votedFor; }

    public long commitIndex() { return commitIndex; }

    public long lastLogIndex() { return log.size(); }

    public int quorum() { return quorum; }

    private long lastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).term();
    }

    private long termAt(long index) {
        return index == 0 ? 0 : log.get((int) index - 1).term();
    }

    // ------------------------------------------------------------------ timer events

    /**
     * Election timer fired. A leader uses it as its check-quorum tick: it steps down unless a majority of the
     * cluster (counting itself) answered it since the previous tick. Any other node starts a PRE-VOTE round: it
     * polls the cluster for the term it would use, without touching its own term or vote, and only starts a real
     * election if a majority says it would grant the vote. A node that cannot reach a majority therefore never
     * inflates its term.
     */
    public List<Action> onElectionTimeout() {
        List<Action> actions = new ArrayList<>();
        if (state == State.LEADER) {
            actions.add(new Action.ResetElectionTimer());
            if (heardFrom.size() + 1 < quorum) {
                state = State.FOLLOWER; // cannot reach a majority: stop acting as leader
                leaderId = null;
            }
            heardFrom.clear();
            return actions;
        }
        heardLeader = false;
        state = State.PRE_CANDIDATE;
        leaderId = null;
        votes.clear();
        preVotes.clear();
        preVotes.add(selfId);
        actions.add(new Action.ResetElectionTimer());
        if (preVotes.size() >= quorum) { // single-node cluster
            startElection(actions);
            return finish(actions);
        }
        RequestVoteRequest poll = new RequestVoteRequest(currentTerm + 1, selfId, lastLogIndex(), lastLogTerm());
        for (String peer : peerIds) {
            actions.add(new Action.SendPreVote(peer, poll));
        }
        return actions;
    }

    /** A pre-vote majority was reached: start the real election in the next term. */
    private void startElection(List<Action> actions) {
        currentTerm++;
        votedFor = selfId;
        hardStateDirty = true;
        state = State.CANDIDATE;
        leaderId = null;
        preVotes.clear();
        votes.clear();
        votes.add(selfId);
        if (votes.size() >= quorum) { // single-node cluster
            becomeLeader(actions);
            return;
        }
        RequestVoteRequest request = new RequestVoteRequest(currentTerm, selfId, lastLogIndex(), lastLogTerm());
        for (String peer : peerIds) {
            actions.add(new Action.SendRequestVote(peer, request));
        }
    }

    /** Heartbeat timer fired: leaders send AppendEntries (carrying pending entries, if any) to every peer. */
    public List<Action> onHeartbeatTick() {
        List<Action> actions = new ArrayList<>();
        if (state != State.LEADER) {
            return actions;
        }
        for (String peer : peerIds) {
            replicateTo(peer, actions);
        }
        return actions;
    }

    // ------------------------------------------------------------------ RequestVote

    public Outcome<RequestVoteResponse> handleRequestVote(RequestVoteRequest req) {
        List<Action> actions = new ArrayList<>();
        if (req.term() < currentTerm) {
            return new Outcome<>(new RequestVoteResponse(currentTerm, false), finish(actions));
        }
        if (req.term() > currentTerm) {
            stepDown(req.term(), actions);
        }
        // §5.4.1: candidate's log must be at least as up-to-date as ours.
        boolean candidateLogUpToDate = req.lastLogTerm() > lastLogTerm()
                || (req.lastLogTerm() == lastLogTerm() && req.lastLogIndex() >= lastLogIndex());
        boolean knownCandidate = peerIds.contains(req.candidateId());
        boolean canVote = votedFor == null || votedFor.equals(req.candidateId());
        boolean grant = knownCandidate && canVote && candidateLogUpToDate;
        if (grant) {
            if (!req.candidateId().equals(votedFor)) {
                votedFor = req.candidateId();
                hardStateDirty = true;
            }
            actions.add(new Action.ResetElectionTimer());
        }
        return new Outcome<>(new RequestVoteResponse(currentTerm, grant), finish(actions));
    }

    public List<Action> handleRequestVoteResponse(String from, RequestVoteResponse resp) {
        List<Action> actions = new ArrayList<>();
        if (resp.term() > currentTerm) {
            stepDown(resp.term(), actions);
            return finish(actions);
        }
        if (state != State.CANDIDATE || resp.term() != currentTerm || !resp.voteGranted()
                || !peerIds.contains(from)) {
            return actions;
        }
        votes.add(from);
        if (votes.size() >= quorum) {
            becomeLeader(actions);
        }
        return finish(actions);
    }

    // ------------------------------------------------------------------ pre-vote

    /**
     * A pre-vote is only a poll: it never changes this node's term, vote, role or timers. It is granted when the
     * proposed term is ahead of ours and the candidate's log is up to date (and, with leader stickiness, when we
     * have not heard from a live leader recently). The reply carries the proposed term when granted.
     */
    public Outcome<RequestVoteResponse> handlePreVote(RequestVoteRequest req) {
        boolean candidateLogUpToDate = req.lastLogTerm() > lastLogTerm()
                || (req.lastLogTerm() == lastLogTerm() && req.lastLogIndex() >= lastLogIndex());
        boolean knownCandidate = peerIds.contains(req.candidateId());
        boolean leaderAlive = leaderStickiness && (state == State.LEADER || heardLeader);
        boolean grant = knownCandidate && candidateLogUpToDate && req.term() > currentTerm && !leaderAlive;
        return new Outcome<>(new RequestVoteResponse(grant ? req.term() : currentTerm, grant), new ArrayList<>());
    }

    public List<Action> handlePreVoteResponse(String from, RequestVoteRequest poll, RequestVoteResponse resp) {
        List<Action> actions = new ArrayList<>();
        if (!resp.voteGranted()) {
            if (resp.term() > currentTerm) {
                stepDown(resp.term(), actions); // we were behind: adopt the newer term
            }
            return finish(actions);
        }
        if (state != State.PRE_CANDIDATE || poll.term() != currentTerm + 1 || !peerIds.contains(from)) {
            return actions; // stale answer to an earlier round
        }
        preVotes.add(from);
        if (preVotes.size() >= quorum) {
            startElection(actions);
        }
        return finish(actions);
    }

    // ------------------------------------------------------------------ AppendEntries (follower side)

    public Outcome<AppendEntriesResponse> handleAppendEntries(AppendEntriesRequest req) {
        List<Action> actions = new ArrayList<>();
        if (req.term() < currentTerm) {
            return new Outcome<>(reject(lastLogIndex() + 1), finish(actions));
        }
        if (req.term() > currentTerm) {
            stepDown(req.term(), actions);
        } else if (state == State.CANDIDATE || state == State.PRE_CANDIDATE) {
            state = State.FOLLOWER; // a legitimate leader exists for our own term
            votes.clear();
            preVotes.clear();
        } else if (state == State.LEADER) {
            // Two leaders in one term would violate Election Safety; never accept entries from one.
            return new Outcome<>(reject(lastLogIndex() + 1), finish(actions));
        }
        leaderId = req.leaderId();
        heardLeader = true;
        actions.add(new Action.ResetElectionTimer());

        long prev = req.prevLogIndex();
        List<LogEntry> incoming = req.entries();
        for (int i = 0; i < incoming.size(); i++) {
            if (incoming.get(i).index() != prev + 1 + i) {
                return new Outcome<>(reject(lastLogIndex() + 1), finish(actions));
            }
        }

        // Log Matching check.
        if (prev > lastLogIndex()) {
            return new Outcome<>(reject(lastLogIndex() + 1), finish(actions));
        }
        if (prev > 0 && termAt(prev) != req.prevLogTerm()) {
            long conflictTerm = termAt(prev);
            long first = prev;
            while (first > 1 && termAt(first - 1) == conflictTerm) {
                first--;
            }
            return new Outcome<>(reject(first), finish(actions));
        }

        // Conflict resolution: overwrite uncommitted entries that disagree with the leader.
        List<LogEntry> toAppend = new ArrayList<>();
        long idx = prev;
        for (LogEntry entry : incoming) {
            idx++;
            if (idx <= lastLogIndex()) {
                if (termAt(idx) == entry.term()) {
                    continue; // already have this exact entry
                }
                if (idx <= commitIndex) {
                    throw new IllegalStateException("leader asked to overwrite committed entry " + idx);
                }
                while (log.size() >= idx) {
                    log.remove(log.size() - 1);
                }
                actions.add(new Action.TruncateLogFrom(idx));
            }
            log.add(entry);
            toAppend.add(entry);
        }
        if (!toAppend.isEmpty()) {
            actions.add(new Action.AppendToLog(toAppend));
        }

        long lastNew = prev + incoming.size();
        if (req.leaderCommit() > commitIndex) {
            long newCommit = Math.min(req.leaderCommit(), lastNew);
            if (newCommit > commitIndex) {
                commitIndex = newCommit;
                emitApplies(actions);
            }
        }
        return new Outcome<>(new AppendEntriesResponse(currentTerm, true, lastNew, 0), finish(actions));
    }

    private AppendEntriesResponse reject(long conflictIndex) {
        return new AppendEntriesResponse(currentTerm, false, 0, conflictIndex);
    }

    // ------------------------------------------------------------------ AppendEntries (leader side)

    public List<Action> handleAppendEntriesResponse(String from, AppendEntriesRequest req,
                                                    AppendEntriesResponse resp) {
        List<Action> actions = new ArrayList<>();
        if (resp.term() > currentTerm) {
            stepDown(resp.term(), actions);
            return finish(actions);
        }
        if (state != State.LEADER || req.term() != currentTerm || !nextIndex.containsKey(from)) {
            return actions; // stale response
        }
        heardFrom.add(from); // any same-term answer proves this leader can still reach that peer
        if (resp.success()) {
            long newMatch = Math.min(resp.matchIndex(), lastLogIndex());
            if (newMatch > matchIndex.get(from)) {
                matchIndex.put(from, newMatch);
                advanceCommitIndex(actions);
            }
            nextIndex.put(from, matchIndex.get(from) + 1);
            if (nextIndex.get(from) <= lastLogIndex()) {
                replicateTo(from, actions); // keep streaming until the follower has caught up
            }
        } else {
            long next = Math.max(1, Math.min(resp.conflictIndex(), nextIndex.get(from) - 1));
            nextIndex.put(from, next);
            replicateTo(from, actions);
        }
        return finish(actions);
    }

    // ------------------------------------------------------------------ client proposals

    /** Leader-only: append a command to the log and start replicating it. */
    public Outcome<ProposeResult> propose(Command command) {
        List<Action> actions = new ArrayList<>();
        if (state != State.LEADER) {
            return new Outcome<>(new ProposeResult(false, 0, currentTerm), actions);
        }
        LogEntry entry = new LogEntry(currentTerm, lastLogIndex() + 1, command);
        log.add(entry);
        actions.add(new Action.AppendToLog(List.of(entry)));
        if (peerIds.isEmpty()) {
            advanceCommitIndex(actions);
        } else {
            for (String peer : peerIds) {
                replicateTo(peer, actions);
            }
        }
        return new Outcome<>(new ProposeResult(true, entry.index(), entry.term()), finish(actions));
    }

    // ------------------------------------------------------------------ internals

    private void becomeLeader(List<Action> actions) {
        state = State.LEADER;
        leaderId = selfId;
        votes.clear();
        preVotes.clear();
        heardFrom.clear();
        nextIndex.clear();
        matchIndex.clear();
        for (String peer : peerIds) {
            nextIndex.put(peer, lastLogIndex() + 1);
            matchIndex.put(peer, 0L);
        }
        // A no-op entry from the new term lets entries from older terms commit indirectly (§5.4.2).
        LogEntry barrier = new LogEntry(currentTerm, lastLogIndex() + 1, Command.noop());
        log.add(barrier);
        actions.add(new Action.AppendToLog(List.of(barrier)));
        actions.add(new Action.ResetElectionTimer()); // doubles as the check-quorum tick
        if (peerIds.isEmpty()) {
            advanceCommitIndex(actions);
        } else {
            for (String peer : peerIds) {
                replicateTo(peer, actions);
            }
        }
    }

    private void stepDown(long newTerm, List<Action> actions) {
        boolean timerMayBeOff = state != State.FOLLOWER;
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            votedFor = null;
            hardStateDirty = true;
            leaderId = null;
        }
        state = State.FOLLOWER;
        votes.clear();
        preVotes.clear();
        if (timerMayBeOff) {
            actions.add(new Action.ResetElectionTimer());
        }
    }

    private void replicateTo(String peer, List<Action> actions) {
        long next = nextIndex.get(peer);
        long prev = next - 1;
        long last = Math.min(lastLogIndex(), prev + MAX_ENTRIES_PER_RPC);
        List<LogEntry> entries = new ArrayList<>(log.subList((int) prev, (int) last));
        actions.add(new Action.SendAppendEntries(peer, new AppendEntriesRequest(
                currentTerm, selfId, prev, termAt(prev), entries, commitIndex)));
    }

    /**
     * §5.4.2: a leader only advances commitIndex by counting replicas of an entry from its CURRENT term.
     * Entries from earlier terms become committed indirectly, as a consequence of a current-term entry
     * above them reaching a quorum. They are never committed by replica counting alone.
     */
    private void advanceCommitIndex(List<Action> actions) {
        for (long n = lastLogIndex(); n > commitIndex; n--) {
            if (termAt(n) != currentTerm) {
                break; // terms never decrease along the log: everything below is also from an older term
            }
            int replicas = 1; // the leader itself
            for (String peer : peerIds) {
                if (matchIndex.get(peer) >= n) {
                    replicas++;
                }
            }
            if (replicas >= quorum) {
                commitIndex = n;
                emitApplies(actions);
                return;
            }
        }
    }

    private void emitApplies(List<Action> actions) {
        if (commitIndex > lastApplied) {
            List<LogEntry> newlyCommitted = new ArrayList<>(log.subList((int) lastApplied, (int) commitIndex));
            lastApplied = commitIndex;
            actions.add(new Action.ApplyEntries(newlyCommitted));
        }
    }

    private List<Action> finish(List<Action> actions) {
        if (hardStateDirty) {
            actions.add(0, new Action.PersistHardState(currentTerm, votedFor));
            hardStateDirty = false;
        }
        return actions;
    }
}
